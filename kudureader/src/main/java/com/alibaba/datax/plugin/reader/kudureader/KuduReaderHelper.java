package com.alibaba.datax.plugin.reader.kudureader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.CharEncoding;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.kudu.shaded.org.checkerframework.checker.units.qual.K;
import org.apache.kudu.util.TimestampUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
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
                    .defaultAdminOperationTimeoutMs(Long.parseLong(conf.get(Key.KUDU_ADMIN_TIMEOUT).toString()))
                    .defaultOperationTimeoutMs(Long.parseLong(conf.get(Key.KUDU_SESSION_TIMEOUT).toString()))
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

    public static Object typeConversion(String value, Type type) throws ParseException {
        Object res = null;
        switch (type) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                res = Long.valueOf(value);
                break;
            case STRING:
                res = value;
                break;
            case BOOL:
                res = Boolean.valueOf(value);
                break;
            case FLOAT:
                res = Float.valueOf(value);
                break;
            case DOUBLE:
                res = Double.valueOf(value);
                break;
            case UNIXTIME_MICROS:
                Long timestamp = null;
                try {
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                    format.setTimeZone(TimeZone.getTimeZone("Europe/London"));
                    timestamp = format.parse(value).getTime();
//                    timestamp = DateUtils.parseDate(value,Locale.UK, "yyyy-MM-dd").getTime();
                } catch (ParseException e) {
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    format.setTimeZone(TimeZone.getTimeZone("Europe/London"));
                    timestamp = format.parse(value).getTime();
//                    timestamp = DateUtils.parseDate(value,Locale.UK, "yyyy-MM-dd HH:mm:ss").getTime();
                }
                res = new Timestamp(timestamp);
                break;
            case DECIMAL:
                res = new BigDecimal(value);
                break;
            default:
                throw DataXException.asDataXException(KuduReaderErrorcode.ILLEGAL_VALUE,
                        String.format("Kudureader does not support the type:%s, currently supported types are:%s", type.getName(), Arrays.asList(ColumnType.values())));
        }

        return res;
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

                    Object columnValue = null;
                    if (null != name) {
                        if (null != rowResult.getObject(name)) {
                            columnValue = rowResult.getObject(name);
                        }
                    } else {
                        columnValue = value;
                    }
                    ColumnType columnType = ColumnType.getByTypeName(type);
                    switch (columnType) {
                        case STRING:
                            try {
                                columnGenerated = new StringColumn(columnValue.toString());
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
                                columnGenerated = new LongColumn(columnValue.toString());
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                        "LONG"));
                            }
                            break;
                        case FLOAT:
                        case DOUBLE:
                            try {
                                columnGenerated = new DoubleColumn(columnValue.toString());
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                        "DOUBLE"));
                            }
                            break;
                        case BOOLEAN:
                            try {
                                columnGenerated = new BoolColumn(columnValue.toString());
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
                                        // 框架尝试转换
                                        columnGenerated = new DateColumn(
                                                new StringColumn(TimestampUtil.timestampToString(Timestamp.valueOf(columnValue.toString())))
                                                        .asDate());
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
                                    String.format("Kudureader does not support the type:%s, currently supported types are:%s",
                                            type, Arrays.asList(ColumnType.values())));
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

    public static List<KuduPredicate> getKuduPredicates(Configuration configuration, KuduTable kuduTable) throws ParseException {
        List<KuduPredicate> predicates = new ArrayList<>();
        String whereSql = configuration.getString(Key.WHERE);
        if (whereSql == null || "".equals(whereSql.trim())) {
            return predicates;
        }
        whereSql=whereSql.replaceAll("[\"']","");
        String[] expressions = whereSql.split("and");
        for (String exp : expressions) {
            Type type = null;
            Object value = null;
                String[] words = exp.split("\\s+");
                if ("".equals(words[0])) {
                    words = Arrays.copyOfRange(words, 1, words.length);
                }
                switch (words[1].charAt(0)) {
                    case '=':
                        type = kuduTable.getSchema().getColumn(words[0]).getType();
                        value = KuduReaderHelper.typeConversion(words[2], type);
                        predicates.add(KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(words[0]),
                                KuduPredicate.ComparisonOp.EQUAL, value));
                        LOG.info("The filtering condition is [{} EQUAL(=) {}]", words[0], words[2]);
                        break;
                    case '<':
                        if (words[1].length() == 1) {
                            type = kuduTable.getSchema().getColumn(words[0]).getType();
                            value = KuduReaderHelper.typeConversion(words[2], type);
                            predicates.add(KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(words[0]),
                                    KuduPredicate.ComparisonOp.LESS, value));
                            LOG.info("The filtering condition is [{} LESS(<) {}]", words[0], words[2]);
                        } else if ("<>".equals(words[1])) {
                            type = kuduTable.getSchema().getColumn(words[0]).getType();
                            value = KuduReaderHelper.typeConversion(words[2], type);
                            predicates.add(KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(words[0]),
                                    KuduPredicate.ComparisonOp.LESS, value));
                            predicates.add(KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(words[0]),
                                    KuduPredicate.ComparisonOp.GREATER, value));
                            LOG.info("The filtering condition is [{} NOT EQUAL(<>) {}]", words[0], words[2]);
                        } else if ("<=".equals(words[1])) {
                            type = kuduTable.getSchema().getColumn(words[0]).getType();
                            value = KuduReaderHelper.typeConversion(words[2], type);
                            predicates.add(KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(words[0]),
                                    KuduPredicate.ComparisonOp.LESS_EQUAL, value));
                            LOG.info("The filtering condition is [{} LESS_EQUAL(<=) {}]", words[0], words[2]);
                        } else {
                            LOG.error("Unsupported where expressions", DataXException.asDataXException(KuduReaderErrorcode.SPLIT_ERROR, "Unsupported where expressions"));
                        }

                        break;
                    case '>':
                        if (words[1].length() == 1) {
                            type = kuduTable.getSchema().getColumn(words[0]).getType();
                            value = KuduReaderHelper.typeConversion(words[2], type);
                            predicates.add(KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(words[0]),
                                    KuduPredicate.ComparisonOp.GREATER, value));
                            LOG.info("The filtering condition is [{} GREATER(>) {}]", words[0], words[2]);
                        } else if (">=".equals(words[1])) {
                            type = kuduTable.getSchema().getColumn(words[0]).getType();
                            value = KuduReaderHelper.typeConversion(words[2], type);
                            predicates.add(KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(words[0]),
                                    KuduPredicate.ComparisonOp.GREATER_EQUAL, value));
                            LOG.info("The filtering condition is [{} GREATER_EQUAL(>=) {}]", words[0], words[2]);
                        } else {
                            LOG.error("Unsupported where expressions", DataXException.asDataXException(KuduReaderErrorcode.SPLIT_ERROR, "Unsupported where expressions"));
                        }
                        break;
                    case 'i':
                        if ("is".equals(words[1]) && "not".equals(words[2]) && "null".equals(words[3])) {
                            predicates.add(KuduPredicate.newIsNotNullPredicate(kuduTable.getSchema().getColumn(words[0])));
                            LOG.info("The filtering condition is [{} IS NOT {}]", words[0], "null");
                        } else if ("is".equals(words[1]) && "null".equals(words[2])) {
                            predicates.add(KuduPredicate.newIsNullPredicate(kuduTable.getSchema().getColumn(words[0])));
                            LOG.info("The filtering condition is [{} IS {}]", words[0], "null");
                        }
//                        else if ("in".equals(words[1])) {
//                            predicates.add(KuduPredicate.newInListPredicate(kuduTable.getSchema().getColumn(words[0]),));
//                            LOG.info("The filtering condition is [{} IS NOT {}]", words[0], "null");
//                        }
                        else {
                            LOG.error("Unsupported where expressions", DataXException.asDataXException(KuduReaderErrorcode.SPLIT_ERROR, "Unsupported where expressions"));
                        }

                        break;
                    case '!':
                        if ("!=".equals(words[1])) {
                            type = kuduTable.getSchema().getColumn(words[0]).getType();
                            value = KuduReaderHelper.typeConversion(words[2], type);
                            predicates.add(KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(words[0]),
                                    KuduPredicate.ComparisonOp.LESS, value));
                            predicates.add(KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(words[0]),
                                    KuduPredicate.ComparisonOp.GREATER, value));
                            LOG.info("The filtering condition is [{} NOT EQUAL(!=) {}]", words[0], words[2]);
                        } else {
                            LOG.error("Unsupported where expressions", DataXException.asDataXException(KuduReaderErrorcode.SPLIT_ERROR, "Unsupported where expressions"));
                        }
                        break;
//                    case 'n':
//                        if ("not".equals(words[1]) && "in".equals(words[2])) {
//                            System.out.println("not in");
//                        } else {
//                            LOG.error("Unsupported where expressions", DataXException.asDataXException(KuduReaderErrorcode.SPLIT_ERROR, "Unsupported where expressions"));
//                        }
//                        break;
                    default:
                        LOG.error("Unsupported where expressions", DataXException.asDataXException(KuduReaderErrorcode.SPLIT_ERROR, "Unsupported where expressions"));
                        break;
                }
        }

        return predicates;
    }

    public static List<Configuration> split(Configuration configuration) {
        LOG.info("Kudureader start split!");
        List<Configuration> splitConfigs = null;
        KuduClient kuduClient = null;
        try {
            kuduClient = KuduReaderHelper.getKuduClient(configuration.getString(Key.KUDU_CONFIG));
            KuduTable kuduTable = KuduReaderHelper.getKuduTable(configuration, kuduClient);

            //目标分区过滤
            List<KuduPredicate> predicates = KuduReaderHelper.getKuduPredicates(configuration, kuduTable);
            KuduScanToken.KuduScanTokenBuilder tokenBuilder = kuduClient.newScanTokenBuilder(kuduTable)
                    .setProjectedColumnNames(KuduReaderHelper.getColumnNames(configuration));
            for (KuduPredicate predicate : predicates) {
                tokenBuilder.addPredicate(predicate);
            }
            List<KuduScanToken> tokens = tokenBuilder.build();
            splitConfigs = new ArrayList<>(tokens.size());

            //目标kudu表有多少个分区，就会分多少片
            for (KuduScanToken token : tokens) {
//                List<String> locations = new ArrayList<>(token.getTablet().getReplicas().size());
//                for (LocatedTablet.Replica replica : token.getTablet().getReplicas()) {
//                    locations.add(replica.getRpcHost());
//                }
                Configuration conf = configuration.clone();
//                conf.set(Key.SPLIT_PK_RPC_HOST, locations);
                conf.set(Key.SPLIT_PK_TOKEN, new String(token.serialize(), CharEncoding.ISO_8859_1));
                splitConfigs.add(conf);
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(KuduReaderErrorcode.SPLIT_ERROR, e);
        } finally {
            KuduReaderHelper.closeClient(kuduClient);
        }
        return splitConfigs;
    }
}
