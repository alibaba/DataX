package com.alibaba.datax.plugin.reader.otsreader;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.callable.GetRangeCallable;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConst;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.alibaba.datax.plugin.reader.otsreader.utils.Common;
import com.alibaba.datax.plugin.reader.otsreader.utils.GsonParser;
import com.alibaba.datax.plugin.reader.otsreader.utils.DefaultNoRetry;
import com.alibaba.datax.plugin.reader.otsreader.utils.RetryHelper;
import com.aliyun.openservices.ots.OTSClientAsync;
import com.aliyun.openservices.ots.OTSServiceConfiguration;
import com.aliyun.openservices.ots.model.Direction;
import com.aliyun.openservices.ots.model.GetRangeRequest;
import com.aliyun.openservices.ots.model.GetRangeResult;
import com.aliyun.openservices.ots.model.OTSFuture;
import com.aliyun.openservices.ots.model.RangeRowQueryCriteria;
import com.aliyun.openservices.ots.model.Row;
import com.aliyun.openservices.ots.model.RowPrimaryKey;

public class OtsReaderSlaveProxy {
    
    class RequestItem {
        private RangeRowQueryCriteria criteria;
        private OTSFuture<GetRangeResult> future;
        
        RequestItem(RangeRowQueryCriteria criteria, OTSFuture<GetRangeResult> future) {
            this.criteria = criteria;
            this.future = future;
        }

        public RangeRowQueryCriteria getCriteria() {
            return criteria;
        }

        public OTSFuture<GetRangeResult> getFuture() {
            return future;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(OtsReaderSlaveProxy.class);
    
    private void rowsToSender(List<Row> rows, RecordSender sender, List<OTSColumn> columns) {
        for (Row row : rows) {
            Record line = sender.createRecord();
            line = Common.parseRowToLine(row, columns, line);
            
            LOG.debug("Reader send record : {}", line.toString());
            
            sender.sendToWriter(line);
        }
    }
    
    private RangeRowQueryCriteria generateRangeRowQueryCriteria(String tableName, RowPrimaryKey begin, RowPrimaryKey end, Direction direction, List<String> columns) {
        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
        criteria.setInclusiveStartPrimaryKey(begin);
        criteria.setDirection(direction);
        criteria.setColumnsToGet(columns);
        criteria.setLimit(-1);
        criteria.setExclusiveEndPrimaryKey(end);
        return criteria;
    }
    
    private RequestItem generateRequestItem(
            OTSClientAsync ots, 
            OTSConf conf, 
            RowPrimaryKey begin, 
            RowPrimaryKey end, 
            Direction direction, 
            List<String> columns) throws Exception {
        RangeRowQueryCriteria criteria = generateRangeRowQueryCriteria(conf.getTableName(), begin, end, direction, columns);
        
        GetRangeRequest request = new GetRangeRequest();
        request.setRangeRowQueryCriteria(criteria);
        OTSFuture<GetRangeResult> future =  ots.getRange(request);
        
        return new RequestItem(criteria, future);
    }

    public void read(RecordSender sender, Configuration configuration) throws Exception {
        LOG.info("read begin.");
        
        OTSConf conf = GsonParser.jsonToConf(configuration.getString(OTSConst.OTS_CONF));
        OTSRange range = GsonParser.jsonToRange(configuration.getString(OTSConst.OTS_RANGE));
        Direction direction = GsonParser.jsonToDirection(configuration.getString(OTSConst.OTS_DIRECTION));
        
        OTSServiceConfiguration configure = new OTSServiceConfiguration();
        configure.setRetryStrategy(new DefaultNoRetry());
        
        OTSClientAsync ots = new OTSClientAsync(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccesskey(),
                conf.getInstanceName(),
                null,
                configure,
                null);
        
        RowPrimaryKey token = range.getBegin();
        List<String> columns = Common.getNormalColumnNameList(conf.getColumns());
        
        RequestItem request = null;
        
        do {
            LOG.debug("Next token : {}", GsonParser.rowPrimaryKeyToJson(token));
            if (request == null) {
                request = generateRequestItem(ots, conf, token, range.getEnd(), direction, columns);
            } else {
                RequestItem req = request;

                GetRangeResult result = RetryHelper.executeWithRetry(
                        new GetRangeCallable(ots, req.getCriteria(), req.getFuture()),
                        conf.getRetry(),
                        conf.getSleepInMilliSecond()
                    );
                if ((token = result.getNextStartPrimaryKey()) != null) {
                    request = generateRequestItem(ots, conf, token, range.getEnd(), direction, columns);
                }
                
                rowsToSender(result.getRows(), sender, conf.getColumns());
            }
        } while (token != null);
        ots.shutdown();
        LOG.info("read end.");
    }
}
