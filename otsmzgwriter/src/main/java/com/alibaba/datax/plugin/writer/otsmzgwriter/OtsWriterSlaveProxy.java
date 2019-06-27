package com.alibaba.datax.plugin.writer.otsmzgwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.otsmzgwriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otsmzgwriter.model.OTSConst;
import com.alibaba.datax.plugin.writer.otsmzgwriter.model.WithRecord;
import com.alibaba.datax.plugin.writer.otsmzgwriter.utils.Common;
import com.alibaba.datax.plugin.writer.otsmzgwriter.utils.GsonParser;
import com.aliyun.openservices.ots.*;
import com.aliyun.openservices.ots.internal.OTSCallback;
import com.aliyun.openservices.ots.internal.writer.WriterConfig;
import com.aliyun.openservices.ots.model.*;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;


public class OtsWriterSlaveProxy {

    private static final Logger LOG = LoggerFactory.getLogger(OtsWriterSlaveProxy.class);
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
            WithRecord withRecord = (WithRecord) otsContext.getOTSRequest();
            collector.collectDirtyRecord(withRecord.getRecord(), ex);
        }

        @Override
        public void onFailed(OTSContext<RowChange, ConsumedCapacity> otsContext, ClientException ex) {
            LOG.error("Write row failed.", ex);
            WithRecord withRecord = (WithRecord) otsContext.getOTSRequest();
            collector.collectDirtyRecord(withRecord.getRecord(), ex);
        }
    }

    public void init(Configuration configuration) {
        conf = GsonParser.jsonToConf(configuration.getString(OTSConst.OTS_CONF));

        ClientConfiguration clientConfigure = new ClientConfiguration();
        clientConfigure.setIoThreadCount(conf.getIoThreadCount());
        clientConfigure.setMaxConnections(conf.getConcurrencyWrite());
        clientConfigure.setSocketTimeoutInMillisecond(conf.getSocketTimeout());
        clientConfigure.setConnectionTimeoutInMillisecond(conf.getConnectTimeout());

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

    public void close() {
        otsAsync.shutdown();
    }

    /**
     * 写Pooled Table的通用处理方式
     *
     * @param recordReceiver
     * @param collector
     * @throws Exception
     */
    public void write(RecordReceiver recordReceiver, TaskPluginCollector collector) throws Exception {
        LOG.info("Writer slave started.");

        WriterConfig writerConfig = new WriterConfig();
        writerConfig.setConcurrency(conf.getConcurrencyWrite());
        writerConfig.setMaxBatchRowsCount(conf.getBatchWriteCount());
        writerConfig.setMaxBatchSize(conf.getRestrictConf().getRequestTotalSizeLimition());
        writerConfig.setBufferSize(conf.getBufferSize());
        writerConfig.setMaxAttrColumnSize(conf.getRestrictConf().getAttributeColumnSize());
        writerConfig.setMaxColumnsCount(conf.getRestrictConf().getMaxColumnsCount());
        writerConfig.setMaxPKColumnSize(conf.getRestrictConf().getPrimaryKeyColumnSize());
        otsWriter = new DefaultOTSWriter(otsAsync, conf.getTableName(), writerConfig, new WriterCallback(collector), Executors.newFixedThreadPool(3));

        Record record;
        while ((record = recordReceiver.getFromReader()) != null) {
            LOG.debug("Record Raw: {}", record.toString());

            // 类型转换
            try {
                List<Pair<String, ColumnValue>> attributes = Common.getAttrFromRecord(conf.getAttributeColumn(), record);
                RowPrimaryKey primaryKey = Common.getPKFromRecord(conf, attributes);
                RowChange rowChange = Common.columnValuesToRowChange(conf.getTableName(), conf.getOperation(), primaryKey, attributes);
                WithRecord withRecord = (WithRecord) rowChange;
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
