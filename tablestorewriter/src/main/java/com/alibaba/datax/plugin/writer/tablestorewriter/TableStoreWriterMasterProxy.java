package com.alibaba.datax.plugin.writer.tablestorewriter;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.tablestorewriter.callable.GetTableMetaCallable;
import com.alibaba.datax.plugin.writer.tablestorewriter.model.TableStoreOpType;
import com.alibaba.datax.plugin.writer.tablestorewriter.model.TableStoreConfig;
import com.alibaba.datax.plugin.writer.tablestorewriter.model.TableStoreConfig.RestrictConfig;
import com.alibaba.datax.plugin.writer.tablestorewriter.model.TableStoreConst;
import com.alibaba.datax.plugin.writer.tablestorewriter.utils.GsonParser;
import com.alibaba.datax.plugin.writer.tablestorewriter.utils.ParamChecker;
import com.alibaba.datax.plugin.writer.tablestorewriter.utils.RetryHelper;
import com.alibaba.datax.plugin.writer.tablestorewriter.utils.WriterModelParser;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.TableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TableStoreWriterMasterProxy {

    private TableStoreConfig tableStoreConfig = new TableStoreConfig();

    private SyncClient syncClient = null;

    private TableMeta tableMeta = null;

    private static final Logger LOG = LoggerFactory.getLogger(TableStoreWriterMasterProxy.class);

    /**
     * @param configuration
     * @throws Exception
     */
    public void init(Configuration configuration) throws Exception {

        // 默认参数
        tableStoreConfig.setRetry(configuration.getInt(TableStoreConst.RETRY, 18));
        tableStoreConfig.setSleepInMillisecond(configuration.getInt(TableStoreConst.SLEEP_IN_MILLISECOND, 100));
        tableStoreConfig.setBatchWriteCount(configuration.getInt(TableStoreConst.BATCH_WRITE_COUNT, 100));
        tableStoreConfig.setConcurrencyWrite(configuration.getInt(TableStoreConst.CONCURRENCY_WRITE, 5));
        tableStoreConfig.setIoThreadCount(configuration.getInt(TableStoreConst.IO_THREAD_COUNT, 1));
        tableStoreConfig.setSocketTimeout(configuration.getInt(TableStoreConst.SOCKET_TIMEOUT, 20000));
        tableStoreConfig.setConnectTimeout(configuration.getInt(TableStoreConst.CONNECT_TIMEOUT, 10000));
        tableStoreConfig.setBufferSize(configuration.getInt(TableStoreConst.BUFFER_SIZE, 1024));

        RestrictConfig restrictConfig = tableStoreConfig.new RestrictConfig();
        restrictConfig.setRequestTotalSizeLimitation(configuration.getInt(TableStoreConst.REQUEST_TOTAL_SIZE_LIMITATION, 1024 * 1024));
        restrictConfig.setAttributeColumnSize(configuration.getInt(TableStoreConst.ATTRIBUTE_COLUMN_SIZE_LIMITATION, 2 * 1024 * 1024));
        restrictConfig.setPrimaryKeyColumnSize(configuration.getInt(TableStoreConst.PRIMARY_KEY_COLUMN_SIZE_LIMITATION, 1024));
        restrictConfig.setMaxColumnsCount(configuration.getInt(TableStoreConst.ATTRIBUTE_COLUMN_MAX_COUNT, 1024));
        tableStoreConfig.setRestrictConfig(restrictConfig);

        // 必选参数
        tableStoreConfig.setEndpoint(ParamChecker.checkStringAndGet(configuration, Key.TABLE_STORE_ENDPOINT));
        tableStoreConfig.setAccessId(ParamChecker.checkStringAndGet(configuration, Key.TABLE_STORE_ACCESS_ID));
        tableStoreConfig.setAccessKey(ParamChecker.checkStringAndGet(configuration, Key.TABLE_STORE_ACCESS_KEY));
        tableStoreConfig.setInstanceName(ParamChecker.checkStringAndGet(configuration, Key.TABLE_STORE_INSTANCE_NAME));
        tableStoreConfig.setTableName(ParamChecker.checkStringAndGet(configuration, Key.TABLE_NAME));
        tableStoreConfig.setTableLogicalName(ParamChecker.checkStringAndGet(configuration, Key.TABLE_LOGICAL_NAME));

        tableStoreConfig.setOperation(WriterModelParser.parseTableStoreOpType(ParamChecker.checkStringAndGet(configuration, Key.WRITE_MODE)));

        syncClient = new SyncClient(
                this.tableStoreConfig.getEndpoint(),
                this.tableStoreConfig.getAccessId(),
                this.tableStoreConfig.getAccessKey(),
                this.tableStoreConfig.getInstanceName());

        tableMeta = getTableMeta(syncClient, tableStoreConfig.getTableName());
        LOG.info("Table Meta : {}", GsonParser.metaToJson(tableMeta));

        tableStoreConfig.setPrimaryKeyColumn(WriterModelParser.parseTableStorePKColumnList(ParamChecker.checkListAndGet(configuration, Key.PRIMARY_KEY, true)));
        ParamChecker.checkPrimaryKey(tableMeta, tableStoreConfig.getPrimaryKeyColumn());

        tableStoreConfig.setAttrColumn(WriterModelParser.parseTableStoreAttrColumnList(ParamChecker.checkListAndGet(configuration, Key.COLUMN, tableStoreConfig.getOperation() == TableStoreOpType.UPDATE_ROW ? true : false)));
        ParamChecker.checkAttribute(tableStoreConfig.getAttrColumn());
    }

    public List<Configuration> split(int mandatoryNumber) {
        LOG.info("Begin split and MandatoryNumber : {}", mandatoryNumber);
        List<Configuration> configurations = new ArrayList<Configuration>();
        for (int i = 0; i < mandatoryNumber; i++) {
            Configuration configuration = Configuration.newDefault();
            configuration.set(TableStoreConst.TABLE_STORE_CONFIG, GsonParser.confToJson(this.tableStoreConfig));
            configurations.add(configuration);
        }
        LOG.info("End split.");
        assert (mandatoryNumber == configurations.size());
        return configurations;
    }

    public void close() {
        syncClient.shutdown();
    }

    public TableStoreConfig getTableStoreConfig() {
        return tableStoreConfig;
    }

    private TableMeta getTableMeta(SyncClient syncClient, String tableName) throws Exception {
        return RetryHelper.executeWithRetry(
                new GetTableMetaCallable(syncClient, tableName),
                tableStoreConfig.getRetry(),
                tableStoreConfig.getSleepInMillisecond()
        );
    }
}
