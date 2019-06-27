package com.alibaba.datax.plugin.writer.tablestorewriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.tablestorewriter.model.TableStoreConfig;
import com.alibaba.datax.plugin.writer.tablestorewriter.model.TableStoreConst;
import com.alibaba.datax.plugin.writer.tablestorewriter.model.WithRecord;
import com.alibaba.datax.plugin.writer.tablestorewriter.utils.Common;
import com.alibaba.datax.plugin.writer.tablestorewriter.utils.GsonParser;
import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.TableStoreWriter;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.writer.WriterConfig;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;


public class TableStoreWriterSlaveProxy {

    private static final Logger LOG = LoggerFactory.getLogger(TableStoreWriterSlaveProxy.class);
    private TableStoreConfig tableStoreConfig;
    private AsyncClient asyncClient;
    private TableStoreWriter tableStoreWriter;

    private class WriterCallback implements TableStoreCallback<RowChange, ConsumedCapacity> {

        private TaskPluginCollector collector;

        public WriterCallback(TaskPluginCollector collector) {
            this.collector = collector;
        }

        @Override
        public void onCompleted(RowChange req, ConsumedCapacity res) {
            LOG.debug("Write row succeed. PrimaryKey: {}.", req.getPrimaryKey());
        }

        @Override
        public void onFailed(RowChange req, Exception ex) {
            LOG.error("Write row failed.", ex);
        }
    }

    public void init(Configuration configuration) {
        tableStoreConfig = GsonParser.jsonToConf(configuration.getString(TableStoreConst.TABLE_STORE_CONFIG));

        ClientConfiguration clientConfigure = new ClientConfiguration();
        clientConfigure.setIoThreadCount(tableStoreConfig.getIoThreadCount());
        clientConfigure.setMaxConnections(tableStoreConfig.getConcurrencyWrite());
        clientConfigure.setSocketTimeoutInMillisecond(tableStoreConfig.getSocketTimeout());
        clientConfigure.setConnectionTimeoutInMillisecond(tableStoreConfig.getConnectTimeout());
        clientConfigure.setRetryStrategy(new AlwaysRetryStrategy());

        asyncClient = new AsyncClient(
                tableStoreConfig.getEndpoint(),
                tableStoreConfig.getAccessId(),
                tableStoreConfig.getAccessKey(),
                tableStoreConfig.getInstanceName(),
                clientConfigure);
    }

    public void close() {
        asyncClient.shutdown();
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
        writerConfig.setConcurrency(tableStoreConfig.getConcurrencyWrite());
        writerConfig.setMaxBatchRowsCount(tableStoreConfig.getBatchWriteCount());
        writerConfig.setMaxBatchSize(tableStoreConfig.getRestrictConfig().getRequestTotalSizeLimitation());
        writerConfig.setBufferSize(tableStoreConfig.getBufferSize());
        writerConfig.setMaxAttrColumnSize(tableStoreConfig.getRestrictConfig().getAttributeColumnSize());
        writerConfig.setMaxColumnsCount(tableStoreConfig.getRestrictConfig().getMaxColumnsCount());
        writerConfig.setMaxPKColumnSize(tableStoreConfig.getRestrictConfig().getPrimaryKeyColumnSize());
        tableStoreWriter = new DefaultTableStoreWriter(asyncClient, tableStoreConfig.getTableName(), writerConfig,
                new WriterCallback(collector), Executors.newFixedThreadPool(3));

        Record record;
        while ((record = recordReceiver.getFromReader()) != null) {
            LOG.debug("Record Raw: {}", record.toString());

            // 类型转换
            try {
                List<Pair<String, ColumnValue>> attributes = Common.getAttrFromRecord(tableStoreConfig.getAttrColumn(), record);
                PrimaryKey primaryKey = Common.getPKFromRecord(tableStoreConfig, attributes);
                RowChange rowChange = Common.columnValuesToRowChange(tableStoreConfig.getTableName(), tableStoreConfig.getOperation(), primaryKey, attributes);
                WithRecord withRecord = (WithRecord) rowChange;
                withRecord.setRecord(record);
                tableStoreWriter.addRowChange(rowChange);
            } catch (IllegalArgumentException e) {
                LOG.warn("Found dirty data.", e);
                collector.collectDirtyRecord(record, e.getMessage());
            } catch (ClientException e) {
                LOG.warn("Found dirty data.", e);
                collector.collectDirtyRecord(record, e.getMessage());
            }
        }

        tableStoreWriter.close();
        LOG.info("Writer slave finished.");
    }
}
