package com.alibaba.datax.plugin.writer.adswriter.insert;

import com.alibaba.cloud.analyticdb.adbclient.AdbClient;
import com.alibaba.cloud.analyticdb.adbclient.AdbClientException;
import com.alibaba.cloud.analyticdb.adbclient.DatabaseConfig;
import com.alibaba.cloud.analyticdb.adbclient.Row;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.writer.adswriter.AdsWriterErrorCode;
import com.alibaba.datax.plugin.writer.adswriter.ads.TableInfo;
import com.alibaba.datax.plugin.writer.adswriter.util.Constant;
import com.alibaba.datax.plugin.writer.adswriter.util.Key;
import com.alibaba.fastjson2.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Types;
import java.util.*;

public class AdsClientProxy implements AdsProxy {

    private static final Logger LOG = LoggerFactory.getLogger(AdsClientProxy.class);

    private String table;
    private TaskPluginCollector taskPluginCollector;
    public Configuration configuration;

    // columnName: <java sql type, ads type name>
    private Map<String, Pair<Integer, String>> adsTableColumnsMetaData;
    private Map<String, Pair<Integer, String>> userConfigColumnsMetaData;
    private boolean useRawData[];

    private AdbClient adbClient;

    /**
     * warn: not support columns as *
     */
    public AdsClientProxy(String table, List<String> columns, Configuration configuration,
                          TaskPluginCollector taskPluginCollector, TableInfo tableInfo) {
        this.configuration = configuration;
        this.taskPluginCollector = taskPluginCollector;

        this.adsTableColumnsMetaData = AdsInsertUtil.getColumnMetaData(tableInfo, columns);
        this.userConfigColumnsMetaData = new HashMap<String, Pair<Integer, String>>();
        List<String> adsColumnsNames = tableInfo.getColumnsNames();
        // 要使用用户配置的column顺序
        this.useRawData = new boolean[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            String oriEachColumn = columns.get(i);
            String eachColumn = oriEachColumn;
            // 防御性保留字
            if (eachColumn.startsWith(Constant.ADS_QUOTE_CHARACTER)
                    && eachColumn.endsWith(Constant.ADS_QUOTE_CHARACTER)) {
                eachColumn = eachColumn.substring(1, eachColumn.length() - 1);
            }
            for (String eachAdsColumn : adsColumnsNames) {
                if (eachColumn.equalsIgnoreCase(eachAdsColumn)) {
                    Pair<Integer, String> eachMeta = this.adsTableColumnsMetaData.get(eachAdsColumn);
                    this.userConfigColumnsMetaData.put(oriEachColumn, eachMeta);
                    int columnSqltype = eachMeta.getLeft();
                    switch (columnSqltype) {
                    case Types.DATE:
                    case Types.TIME:
                    case Types.TIMESTAMP:
                        this.useRawData[i] = false;
                        break;
                    default:
                        this.useRawData[i] = true;
                        break;
                    }
                }
            }
        }

        DatabaseConfig databaseConfig = new DatabaseConfig();
        String url = configuration.getString(Key.ADS_URL);
        String[] hostAndPort = StringUtils.split(url, ":");
        if (hostAndPort.length != 2) {
            throw DataXException.asDataXException(AdsWriterErrorCode.INVALID_CONFIG_VALUE,
                    "url should be in host:port format!");
        }
        this.table = table.toLowerCase();
        databaseConfig.setHost(hostAndPort[0]);
        databaseConfig.setPort(Integer.parseInt(hostAndPort[1]));
        databaseConfig.setUser(configuration.getString(Key.USERNAME));
        databaseConfig.setPassword(configuration.getString(Key.PASSWORD));
        databaseConfig.setDatabase(configuration.getString(Key.SCHEMA));
        databaseConfig.setTable(Collections.singletonList(this.table));
        databaseConfig.setColumns(this.table, columns);

        // 如果出现insert失败，是否跳过
        boolean ignoreInsertError = configuration.getBool("ignoreInsertError", false);
        databaseConfig.setIgnoreInsertError(ignoreInsertError);

        // If the value of column is empty, set null
        boolean emptyAsNull = configuration.getBool(Key.EMPTY_AS_NULL, false);
        databaseConfig.setEmptyAsNull(emptyAsNull);

        // 使用insert ignore into方式进行插入
        boolean ignoreInsert = configuration.getBool(Key.IGNORE_INSERT, false);
        databaseConfig.setInsertIgnore(ignoreInsert);

        // commit时，写入ADB出现异常时重试的3次
        int retryTimes = configuration.getInt(Key.RETRY_CONNECTION_TIME, Constant.DEFAULT_RETRY_TIMES);
        databaseConfig.setRetryTimes(retryTimes);

        // 重试间隔的时间为1s，单位是ms
        int retryIntervalTime = configuration.getInt(Key.RETRY_INTERVAL_TIME, 1000);
        databaseConfig.setRetryIntervalTime(retryIntervalTime);

        // 设置自动提交的SQL长度（单位Byte），默认为32KB，一般不建议设置
        int commitSize = configuration.getInt("commitSize", 32768);
        databaseConfig.setCommitSize(commitSize);

        // sdk默认为true
        boolean partitionBatch = configuration.getBool("partitionBatch", true);
        databaseConfig.setPartitionBatch(partitionBatch);

        // 设置写入adb时的并发线程数，默认4，针对配置的所有表
        int parallelNumber = configuration.getInt("parallelNumber", 4);
        databaseConfig.setParallelNumber(parallelNumber);

        // 设置client中使用的logger对象，此处使用slf4j.Logger
        databaseConfig.setLogger(AdsClientProxy.LOG);

        // 设置在拼接insert sql时是否需要带上字段名，默认为true
        boolean insertWithColumnName = configuration.getBool("insertWithColumnName", true);
        databaseConfig.setInsertWithColumnName(insertWithColumnName);

        // sdk 默认值为true
        boolean shareDataSource = configuration.getBool("shareDataSource", true);
        databaseConfig.setShareDataSource(shareDataSource);

        String password = databaseConfig.getPassword();
        databaseConfig.setPassword(password.replaceAll(".", "*"));
        // 避免敏感信息直接打印
        LOG.info("Adb database config is : {}", JSON.toJSONString(databaseConfig));
        databaseConfig.setPassword(password);

        // Initialize AdbClient，初始化实例之后，databaseConfig的配置信息不能再修改
        this.adbClient = new AdbClient(databaseConfig);
    }

    @Override
    public void startWriteWithConnection(RecordReceiver recordReceiver, Connection connection, int columnNumber) {
        try {
            Record record;
            while ((record = recordReceiver.getFromReader()) != null) {

                Row row = new Row();
                List<Object> values = new ArrayList<Object>();
                this.prepareColumnTypeValue(record, values);
                row.setColumnValues(values);

                try {
                    this.adbClient.addRow(this.table, row);
                } catch (AdbClientException e) {
                    if (101 == e.getCode()) {
                        for (String each : e.getErrData()) {
                            Record dirtyData = new DefaultRecord();
                            dirtyData.addColumn(new StringColumn(each));
                            this.taskPluginCollector.collectDirtyRecord(dirtyData, e.getMessage());
                        }
                    } else {
                        throw e;
                    }
                }
            }

            try {
                this.adbClient.commit();
            } catch (AdbClientException e) {
                if (101 == e.getCode()) {
                    for (String each : e.getErrData()) {
                        Record dirtyData = new DefaultRecord();
                        dirtyData.addColumn(new StringColumn(each));
                        this.taskPluginCollector.collectDirtyRecord(dirtyData, e.getMessage());
                    }
                } else {
                    throw e;
                }
            }

        } catch (Exception e) {
            throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
        } finally {
            DBUtil.closeDBResources(null, null, connection);
        }
    }

    private void prepareColumnTypeValue(Record record, List<Object> values) {
        for (int i = 0; i < this.useRawData.length; i++) {
            Column column = record.getColumn(i);
            if (this.useRawData[i]) {
                values.add(column.getRawData());
            } else {
                values.add(column.asString());
            }
        }
    }

    @Override
    public void closeResource() {
        try {
            LOG.info("stop the adbClient");
            this.adbClient.stop();
        } catch (Exception e) {
            LOG.warn("stop adbClient meet a exception, ignore it: {}", e.getMessage(), e);
        }
    }
}
