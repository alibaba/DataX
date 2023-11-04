package com.alibaba.datax.plugin.writer.otswriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConst;
import com.alibaba.datax.plugin.writer.otswriter.utils.WriterRetryPolicy;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSErrorMessage;
import com.alibaba.datax.plugin.writer.otswriter.utils.WithRecord;
import com.alibaba.datax.plugin.writer.otswriter.utils.CommonOld;
import com.alibaba.datax.plugin.writer.otswriter.utils.GsonParser;
import com.aliyun.openservices.ots.*;
import com.aliyun.openservices.ots.internal.OTSCallback;
import com.aliyun.openservices.ots.internal.writer.WriterConfig;
import com.aliyun.openservices.ots.model.*;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;


public class OtsWriterSlaveProxyOld implements IOtsWriterSlaveProxy {

    private static final Logger LOG = LoggerFactory.getLogger(OtsWriterSlaveProxyOld.class);
    private OTSConf conf;
    private OTSAsync otsAsync;
    private OTSWriter otsWriter;

    private class WriterCallback implements OTSCallback<RowChange, ConsumedCapacity> {

        private TaskPluginCollector collector;
        public WriterCallback(TaskPluginCollector collector) {
            this.collector = collector;
        }

        @Override
        public void onCompleted(OTSContext<RowChange, ConsumedCapacity> otsContext) {
            LOG.debug("Write row succeed. PrimaryKey: {}.", otsContext.getOTSRequest().getRowPrimaryKey());
        }

        @Override
        public void onFailed(OTSContext<RowChange, ConsumedCapacity> otsContext, OTSException ex) {
            LOG.error("Write row failed.", ex);
            WithRecord withRecord = (WithRecord)otsContext.getOTSRequest();
            collector.collectDirtyRecord(withRecord.getRecord(), ex);
        }

        @Override
        public void onFailed(OTSContext<RowChange, ConsumedCapacity> otsContext, ClientException ex) {
            LOG.error("Write row failed.", ex);
            WithRecord withRecord = (WithRecord)otsContext.getOTSRequest();
            collector.collectDirtyRecord(withRecord.getRecord(), ex);
        }
    }

    @Override
    public void init(Configuration configuration) {
        conf = GsonParser.jsonToConf(configuration.getString(OTSConst.OTS_CONF));

        ClientConfiguration clientConfigure = new ClientConfiguration();
        clientConfigure.setIoThreadCount(conf.getIoThreadCount());
        clientConfigure.setMaxConnections(conf.getConcurrencyWrite());
        clientConfigure.setSocketTimeoutInMillisecond(conf.getSocketTimeout());
        // TODO
        clientConfigure.setConnectionTimeoutInMillisecond(10000);

        OTSServiceConfiguration otsConfigure = new OTSServiceConfiguration();
        otsConfigure.setRetryStrategy(new WriterRetryPolicy(conf));

        otsAsync = new OTSClientAsync(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstanceName(),
                clientConfigure,
                otsConfigure);
    }

    @Override
    public void close() {
        otsAsync.shutdown();
    }

    @Override
    public void write(RecordReceiver recordReceiver, TaskPluginCollector collector) throws OTSCriticalException {
        LOG.info("Writer slave started.");

        WriterConfig writerConfig = new WriterConfig();
        writerConfig.setConcurrency(conf.getConcurrencyWrite());
        writerConfig.setMaxBatchRowsCount(conf.getBatchWriteCount());
        // TODO
        writerConfig.setMaxBatchSize(1024 * 1024);
        writerConfig.setBufferSize(1024);
        writerConfig.setMaxAttrColumnSize(2 * 1024 * 1024);
        writerConfig.setMaxColumnsCount(1024);
        writerConfig.setMaxPKColumnSize(1024);

        otsWriter = new DefaultOTSWriter(otsAsync, conf.getTableName(), writerConfig, new WriterCallback(collector), Executors.newFixedThreadPool(3));

        int expectColumnCount = conf.getPrimaryKeyColumn().size() + conf.getAttributeColumn().size();
        Record record;
        while ((record = recordReceiver.getFromReader()) != null) {
            LOG.debug("Record Raw: {}", record.toString());

            int columnCount = record.getColumnNumber();
            if (columnCount != expectColumnCount) {
                // 如果Column的个数和预期的个数不一致时，认为是系统故障或者用户配置Column错误，异常退出
                throw new IllegalArgumentException(String.format(OTSErrorMessage.RECORD_AND_COLUMN_SIZE_ERROR, columnCount, expectColumnCount, record.toString()));
            }


            // 类型转换
            try {
                RowPrimaryKey primaryKey = CommonOld.getPKFromRecord(conf.getPrimaryKeyColumn(), record);
                List<Pair<String, ColumnValue>> attributes = CommonOld.getAttrFromRecord(conf.getPrimaryKeyColumn().size(), conf.getAttributeColumn(), record);
                RowChange rowChange = CommonOld.columnValuesToRowChange(conf.getTableName(), conf.getOperation(), primaryKey, attributes);
                WithRecord withRecord = (WithRecord)rowChange;
                withRecord.setRecord(record);
                otsWriter.addRowChange(rowChange);
            } catch (IllegalArgumentException e) {
                LOG.warn("Found dirty data.", e);
                collector.collectDirtyRecord(record, e.getMessage());
            } catch (ClientException e) {
                LOG.warn("Found dirty data.", e);
                collector.collectDirtyRecord(record, e.getMessage());
            }
        }

        otsWriter.close();
        LOG.info("Writer slave finished.");
    }
}
