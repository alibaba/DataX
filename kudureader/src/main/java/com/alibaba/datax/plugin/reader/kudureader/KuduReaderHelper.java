package com.alibaba.datax.plugin.reader.kudureader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.apache.kudu.shaded.org.checkerframework.checker.units.qual.K;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author daizihao
 * @create 2021-01-15 16:18
 **/
public class KuduReaderHelper {
    private static final Logger LOG = LoggerFactory.getLogger(KuduReaderHelper.class);

    public static void validateParameter(Configuration configuration) {
        LOG.info("Start validating parameters！");
        configuration.getNecessaryValue(Key.KUDU_CONFIG, KuduReaderErrorcode.REQUIRED_VALUE);
        configuration.getNecessaryValue(Key.TABLE, KuduReaderErrorcode.REQUIRED_VALUE);
        configuration.getNecessaryValue(Key.COLUMN, KuduReaderErrorcode.REQUIRED_VALUE);
    }

    public static Map<String, Object> getKuduConfiguration(String kuduConfig) {
        if (StringUtils.isBlank(kuduConfig)) {
            throw DataXException.asDataXException(KuduReaderErrorcode.REQUIRED_VALUE,
                    "Connection configuration information required.");
        }
        Map<String, Object> kConfiguration;
        try {
            kConfiguration = JSON.parseObject(kuduConfig, HashMap.class);
            Validate.isTrue(kConfiguration != null, "kuduConfig is null!");
            kConfiguration.put(Key.KUDU_ADMIN_TIMEOUT, kConfiguration.getOrDefault(Key.KUDU_ADMIN_TIMEOUT, Constant.ADMIN_TIMEOUTMS));
            kConfiguration.put(Key.KUDU_SESSION_TIMEOUT, kConfiguration.getOrDefault(Key.KUDU_SESSION_TIMEOUT, Constant.SESSION_TIMEOUTMS));
        } catch (Exception e) {
            throw DataXException.asDataXException(KuduReaderErrorcode.GET_KUDU_CONNECTION_ERROR, e);
        }

        return kConfiguration;
    }

    public static KuduClient getKuduClient(String kuduConfig) {
        Map<String, Object> conf = KuduReaderHelper.getKuduConfiguration(kuduConfig);
        KuduClient kuduClient = null;
        try {
            String masterAddress = (String) conf.get(Key.KUDU_MASTER);
            kuduClient = new KuduClient.KuduClientBuilder(masterAddress)
                    .defaultAdminOperationTimeoutMs((Long) conf.get(Key.KUDU_ADMIN_TIMEOUT))
                    .defaultOperationTimeoutMs((Long) conf.get(Key.KUDU_SESSION_TIMEOUT))
                    .build();
        } catch (Exception e) {
            throw DataXException.asDataXException(KuduReaderErrorcode.GET_KUDU_CONNECTION_ERROR, e);
        }
        return kuduClient;
    }

    public static KuduTable getKuduTable(Configuration configuration, KuduClient kuduClient) {
        String tableName = configuration.getString(Key.TABLE);

        KuduTable table = null;
        try {
            table = kuduClient.openTable(tableName);
        } catch (Exception e) {
            throw DataXException.asDataXException(KuduReaderErrorcode.GET_KUDU_TABLE_ERROR, e);
        }
        return table;
    }

    public static boolean isTableExists(Configuration configuration) {
        String tableName = configuration.getString(Key.TABLE);
        String kuduConfig = configuration.getString(Key.KUDU_CONFIG);
        KuduClient kuduClient = KuduReaderHelper.getKuduClient(kuduConfig);
        try {
            return kuduClient.tableExists(tableName);
        } catch (Exception e) {
            throw DataXException.asDataXException(KuduReaderErrorcode.GET_KUDU_CONNECTION_ERROR,
                    "Please check the table name, the general format is [namespace::database.tablename]!");
        } finally {
            KuduReaderHelper.closeClient(kuduClient);
        }
    }

    public static void closeClient(KuduClient kuduClient) {
        try {
            if (kuduClient != null) {
                kuduClient.close();
            }
        } catch (KuduException e) {
            LOG.warn("The \"kudu client\" was not stopped gracefully. !");

        }
    }

    public static List<String> getColumnNames(Configuration configuration) {

        List<Configuration> columns = configuration.getListConfiguration(Key.COLUMN);
        if (columns == null || columns.toString().contains("\"*\"")) {
            return null;
        }
        ArrayList<String> columnNames = new ArrayList<>();
        for (Configuration column : columns) {
            String name = column.getString(Key.NAME);
            if (StringUtils.isNotBlank(name)) {
                columnNames.add(name);
            }
        }
        return columnNames;
    }

    public static Record transportOneRecord(List<Configuration> columnConfigs, RowResult rowResult
            , RecordSender recordSender, TaskPluginCollector taskPluginCollector, boolean isReadAllColumns) {
        Record record = recordSender.createRecord();
        Column columnGenerated;
        try {
            if (isReadAllColumns) {
                // 读取所有列，创建都为String类型的column
                List<ColumnSchema> columns = rowResult.getSchema().getColumns();
                for (Configuration columnConfig : columnConfigs) {
                    if ("\"*\"".equals(columnConfig.toString().trim())) {
                        for (ColumnSchema column : columns) {
                            String columnValue = null;
                            if (rowResult.getString(column.getName()) != null) {
                                columnValue = rowResult.getString(column.getName());
                            }
                            columnGenerated = new StringColumn(columnValue);
                            record.addColumn(columnGenerated);
                        }

                    }
                }
            } else {
                for (Configuration columnConfig : columnConfigs) {
                    String name = columnConfig.getString(Key.NAME);
                    String type = columnConfig.getString(Key.TYPE, "string");
                    String value = columnConfig.getString(Key.VALUE);

                    String columnValue = null;
                    if (null != name) {
                        if (null != rowResult.getString(name)) {
                            columnValue = rowResult.getString(name);
                        }
                    } else {
                        columnValue = value;
                    }
                    ColumnType columnType = ColumnType.getByTypeName(type);
                    switch (columnType) {
                        case STRING:
                            try {
                                columnGenerated = new StringColumn(columnValue);
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                        "STRING"));
                            }
                            break;
                        case INT:
                        case LONG:
                        case BIGINT:
                            try {
                                columnGenerated = new LongColumn(columnValue);
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                        "LONG"));
                            }

                        case DOUBLE:
                            try {
                                columnGenerated = new DoubleColumn(columnValue);
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                        "DOUBLE"));
                            }
                            break;
                        case BOOLEAN:
                            try {
                                columnGenerated = new BoolColumn(columnValue);
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                        "BOOLEAN"));
                            }

                            break;
                        case TIMESTAMP:
                        case DATE:
                            try {
                                if (columnValue == null) {
                                    columnGenerated = new DateColumn((Date) null);
                                } else {
                                    String formatString = columnConfig.getString(Key.FORMAT);
                                    if (StringUtils.isNotBlank(formatString)) {
                                        // 用户自己配置的格式转换
                                        SimpleDateFormat format = new SimpleDateFormat(
                                                formatString);
                                        columnGenerated = new DateColumn(
                                                format.parse(columnValue));
                                    } else {
                                        // 框架尝试转换
                                        columnGenerated = new DateColumn(
                                                new StringColumn(columnValue)
                                                        .asDate());
                                    }
                                }
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                        "DATE"));
                            }
                            break;
                        default:
                            String errorMessage = String.format(
                                    "Kudureader does not support the type : [%s]", columnType);
                            LOG.error(errorMessage);
                            throw DataXException.asDataXException(KuduReaderErrorcode.ILLEGAL_VALUE,
                                    String.format("Kudureader does not support the type:%s, currently supported types are:%s", type, Arrays.asList(ColumnType.values())));
                    }
                    record.addColumn(columnGenerated);
                }
            }
            recordSender.sendToWriter(record);
        } catch (IllegalArgumentException iae) {
            taskPluginCollector
                    .collectDirtyRecord(record, iae.getMessage());
        } catch (IndexOutOfBoundsException ioe) {
            taskPluginCollector
                    .collectDirtyRecord(record, ioe.getMessage());
        } catch (Exception e) {
            if (e instanceof DataXException) {
                throw (DataXException) e;
            }
            // 每一种转换失败都是脏数据处理,包括数字格式 & 日期格式
            taskPluginCollector.collectDirtyRecord(record, e.getMessage());
        }

        return record;
    }

    public static List<Configuration> split(Configuration configuration, int adviceNumber) {
        String splitPk = configuration.getString(Key.SPLIT_PK);

        return null;
    }

    public static Pair<Object, Object> getPkRange(Configuration configuration){

        return null;
    }
}
