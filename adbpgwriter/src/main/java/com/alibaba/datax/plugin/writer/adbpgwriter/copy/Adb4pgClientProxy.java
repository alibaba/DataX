package com.alibaba.datax.plugin.writer.adbpgwriter.copy;

import com.alibaba.cloud.analyticdb.adb4pgclient.*;
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
import com.alibaba.datax.plugin.writer.adbpgwriter.util.Adb4pgUtil;
import com.alibaba.datax.plugin.writer.adbpgwriter.util.Constant;
import com.alibaba.datax.plugin.writer.adbpgwriter.util.Key;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
/**
 * @author yuncheng
 */
public class Adb4pgClientProxy implements AdbProxy {
    private static final Logger LOG = LoggerFactory.getLogger(Adb4pgClientProxy.class);

    private Adb4pgClient adb4pgClient;
    private String table;
    private String schema;
    List<String> columns;
    private TableInfo tableInfo;
    private TaskPluginCollector taskPluginCollector;
    private boolean useRawData[];
    public Adb4pgClientProxy(Configuration configuration,TaskPluginCollector  taskPluginCollector) {
        this.taskPluginCollector = taskPluginCollector;

        DatabaseConfig databaseConfig = Adb4pgUtil.convertConfiguration(configuration);

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

        // 设置自动提交的SQL长度（单位Byte），默认为10MB，一般不建议设置
        int commitSize = configuration.getInt("commitSize", 10 * 1024 * 1024);
        databaseConfig.setCommitSize(commitSize);


        // 设置写入adb时的并发线程数，默认4，针对配置的所有表
        int parallelNumber = configuration.getInt("parallelNumber", 4);
        databaseConfig.setParallelNumber(parallelNumber);

        // 设置client中使用的logger对象，此处使用slf4j.Logger
        databaseConfig.setLogger(Adb4pgClientProxy.LOG);

        // sdk 默认值为true
        boolean shareDataSource = configuration.getBool("shareDataSource", true);
        databaseConfig.setShareDataSource(shareDataSource);

        //List<String> columns = configuration.getList(Key.COLUMN, String.class);

        this.table = configuration.getString(com.alibaba.datax.plugin.rdbms.writer.Key.TABLE);
        this.schema = configuration.getString(com.alibaba.datax.plugin.writer.adbpgwriter.util.Key.SCHEMA);
        this.adb4pgClient = new Adb4pgClient(databaseConfig);
        this.columns = databaseConfig.getColumns(table,schema);
        this.tableInfo = adb4pgClient.getTableInfo(table, schema);


        this.useRawData = new boolean[this.columns.size()];
        List<ColumnInfo> columnInfos = tableInfo.getColumns();
        for (int i = 0; i < this.columns.size(); i++) {
            String oriEachColumn = columns.get(i);
            String eachColumn = oriEachColumn;
            // 防御性保留字
            if (eachColumn.startsWith(Constant.COLUMN_QUOTE_CHARACTER)
                    && eachColumn.endsWith(Constant.COLUMN_QUOTE_CHARACTER)) {
                eachColumn = eachColumn.substring(1, eachColumn.length() - 1);
            }
            for (ColumnInfo eachAdsColumn : columnInfos) {
                if (eachColumn.equals(eachAdsColumn.getName())) {

                    int columnSqltype = eachAdsColumn.getDataType().sqlType;
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

    }
    @Override
    public void startWriteWithConnection(RecordReceiver recordReceiver, Connection connection) {
        try {
            Record record;
            while ((record = recordReceiver.getFromReader()) != null) {
                Row row = new Row();
                List<Object> values = new ArrayList<Object>();
                this.prepareColumnTypeValue(record, values);
                row.setColumnValues(values);

                try {
                    this.adb4pgClient.addRow(row,this.table, this.schema);
                } catch (Adb4pgClientException e) {
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
                this.adb4pgClient.commit();
            } catch (Adb4pgClientException e) {
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

        }catch (Exception e) {
            throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
        }finally {
            DBUtil.closeDBResources(null, null, connection);
        }
        return;
    }

    private void prepareColumnTypeValue(Record record, List<Object> values) {
        for (int i = 0; i < this.columns.size(); i++) {
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
            LOG.info("stop the adb4pgClient");
            this.adb4pgClient.stop();
        } catch (Exception e) {
            LOG.warn("stop adbClient meet a exception, ignore it: {}", e.getMessage(), e);
        }
    }
}
