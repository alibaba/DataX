package com.alibaba.datax.plugin.writer.otswriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.otswriter.callable.GetTableMetaCallable;
import com.alibaba.datax.plugin.writer.otswriter.model.*;
import com.alibaba.datax.plugin.writer.otswriter.utils.*;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.TableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.alibaba.datax.plugin.writer.otswriter.utils.Common.getOTSInstance;

public class OtsWriterSlaveProxyNormal implements IOtsWriterSlaveProxy {
    
    private OTSConf conf = null;
    private SyncClientInterface ots = null;
    private OTSSendBuffer buffer = null;
    private Map<PrimaryKeySchema, Integer> pkColumnMapping = null;
    private static final Logger LOG = LoggerFactory.getLogger(OtsWriterSlaveProxyNormal.class);
    private PrimaryKeySchema primaryKeySchema =null;

    
    @Override
    public void init(Configuration configuration) {
        LOG.info("init begin");
        this.conf = GsonParser.jsonToConf(configuration.getString(OTSConst.OTS_CONF));
        this.ots = getOTSInstance(conf);
        if (!conf.isTimeseriesTable()){
            this.pkColumnMapping = Common.getPkColumnMapping(conf.getEncodePkColumnMapping());
        }

        buffer = new OTSSendBuffer(ots, conf);

        if(conf.getEnableAutoIncrement()){
            primaryKeySchema = getAutoIncrementKey();
        }
        LOG.info("init end");
    }

    @Override
    public void close() throws com.alibaba.datax.plugin.writer.otswriter.OTSCriticalException {
        LOG.info("close begin");
        ots.shutdown();
        LOG.info("close end");
    }
    
    @Override
    public void write(RecordReceiver recordReceiver, TaskPluginCollector taskPluginCollector) throws com.alibaba.datax.plugin.writer.otswriter.OTSCriticalException {
        LOG.info("write begin");

        // 初始化全局垃圾回收器
        CollectorUtil.init(taskPluginCollector);
        int expectColumnCount = conf.getAttributeColumn().size();
        if (!conf.isTimeseriesTable()){
            expectColumnCount += conf.getPrimaryKeyColumn().size();
        }
        Record record = null;

        while ((record = recordReceiver.getFromReader()) != null) {

            LOG.debug("Record Raw: {}", record.toString());

            int columnCount = record.getColumnNumber();
            if (columnCount != expectColumnCount) {
                // 如果Column的个数和预期的个数不一致时，认为是系统故障或者用户配置Column错误，异常退出
                throw new OTSCriticalException(String.format(
                        OTSErrorMessage.RECORD_AND_COLUMN_SIZE_ERROR,
                        columnCount,
                        expectColumnCount,
                        record.toString()
                ));
            }
            OTSLine line;

            if(conf.getEnableAutoIncrement()){
                line = ParseRecord.parseNormalRecordToOTSLineWithAutoIncrement(
                        conf.getTableName(),
                        conf.getOperation(),
                        pkColumnMapping,
                        conf.getAttributeColumn(),
                        record,
                        conf.getTimestamp(),
                        primaryKeySchema);
            }
            else if(!conf.isTimeseriesTable()){
                line = ParseRecord.parseNormalRecordToOTSLine(
                        conf.getTableName(),
                        conf.getOperation(),
                        pkColumnMapping,
                        conf.getAttributeColumn(),
                        record,
                        conf.getTimestamp());
            }else{
                line = ParseRecord.parseNormalRecordToOTSLineOfTimeseriesTable(conf.getAttributeColumn(),
                        record, conf.getTimeUnit());
            }


            if (line != null) {
                buffer.write(line);
            }
        }

        buffer.close();
        LOG.info("write end");
    }

    private PrimaryKeySchema getAutoIncrementKey() {
        TableMeta tableMeta = null;
        try {
            tableMeta = RetryHelper.executeWithRetry(
                    new GetTableMetaCallable(ots, conf.getTableName()),
                    conf.getRetry(),
                    conf.getSleepInMillisecond()
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        for (PrimaryKeySchema primaryKeySchema : tableMeta.getPrimaryKeyList()) {
            if(primaryKeySchema.hasOption()){
                return primaryKeySchema;
            }
        }
        return null;
    }

    public void setOts(SyncClientInterface ots){
        this.ots = ots;
    }

    public OTSConf getConf() {
        return conf;
    }

    public void setConf(OTSConf conf) {
        this.conf = conf;
    }

    public void setBuffer(OTSSendBuffer buffer) {
        this.buffer = buffer;
    }

    public void setPkColumnMapping(Map<PrimaryKeySchema, Integer> pkColumnMapping) {
        this.pkColumnMapping = pkColumnMapping;
    }
}
