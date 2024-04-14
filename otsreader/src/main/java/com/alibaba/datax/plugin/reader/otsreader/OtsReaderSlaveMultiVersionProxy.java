package com.alibaba.datax.plugin.reader.otsreader;

import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.alibaba.datax.plugin.reader.otsreader.utils.*;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OtsReaderSlaveMultiVersionProxy implements IOtsReaderSlaveProxy {
    private OTSConf conf = null;
    private OTSRange range = null;
    private TableMeta meta = null;
    private SyncClientInterface ots = null;
    
    private static final Logger LOG = LoggerFactory.getLogger(OtsReaderSlaveMultiVersionProxy.class);
    
    @Override
    public void init(Configuration configuration) {
        conf = GsonParser.jsonToConf((String) configuration.get(Constant.ConfigKey.CONF));
        range = GsonParser.jsonToRange((String) configuration.get(Constant.ConfigKey.RANGE));
        meta = GsonParser.jsonToMeta((String) configuration.get(Constant.ConfigKey.META));
        
        this.ots = OtsHelper.getOTSInstance(conf);
    }
    
    @Override
    public void close() {
        ots.shutdown();
    }
    
    private void sendToDatax(RecordSender recordSender, PrimaryKey pk, Column c) {
        Record line = recordSender.createRecord();
        //-------------------------
        // 四元组 pk, column name, timestamp, value
        //-------------------------
        
        // pk
        for( PrimaryKeyColumn pkc : pk.getPrimaryKeyColumns()) {
            line.addColumn(TranformHelper.otsPrimaryKeyColumnToDataxColumn(pkc));
        }
        // column name
        line.addColumn(new StringColumn(c.getName()));
        // Timestamp
        line.addColumn(new LongColumn(c.getTimestamp())); 
        // Value
        line.addColumn(TranformHelper.otsColumnToDataxColumn(c));
        
        recordSender.sendToWriter(line);
    }
    
    private void sendToDatax(RecordSender recordSender, Row row) {
        PrimaryKey pk = row.getPrimaryKey();
        for (Column c : row.getColumns()) {
            sendToDatax(recordSender, pk, c);
        }
    }
    
    /**
     * 将获取到的数据采用4元组的方式传递给datax
     * @param recordSender
     * @param result
     */
    private void sendToDatax(RecordSender recordSender, GetRangeResponse result) {
        LOG.debug("Per request get row count : " + result.getRows().size());
        for (Row row : result.getRows()) {
            sendToDatax(recordSender, row);
        }
    }
    
    @Override
    public void startRead(RecordSender recordSender) throws Exception {

        PrimaryKey inclusiveStartPrimaryKey = new PrimaryKey(range.getBegin());
        PrimaryKey exclusiveEndPrimaryKey = new PrimaryKey(range.getEnd());
        PrimaryKey next = inclusiveStartPrimaryKey;
        
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(conf.getTableName());
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
        rangeRowQueryCriteria.setDirection(Common.getDirection(range.getBegin(), range.getEnd()));
        rangeRowQueryCriteria.setTimeRange(conf.getMulti().getTimeRange());
        rangeRowQueryCriteria.setMaxVersions(conf.getMulti().getMaxVersion());
        rangeRowQueryCriteria.addColumnsToGet(Common.toColumnToGet(conf.getColumn(), meta));

        do{
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(next);
            GetRangeResponse result = OtsHelper.getRange(
                    ots, 
                    rangeRowQueryCriteria, 
                    conf.getRetry(), 
                    conf.getRetryPauseInMillisecond());
            sendToDatax(recordSender, result);
            next = result.getNextStartPrimaryKey();
        } while(next != null);
    }
}
