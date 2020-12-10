package com.alibaba.datax.plugin.reader.odpsreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.reader.odpsreader.ColumnType;
import com.alibaba.datax.plugin.reader.odpsreader.Constant;
import com.alibaba.datax.plugin.reader.odpsreader.Key;
import com.alibaba.datax.plugin.reader.odpsreader.OdpsReaderErrorCode;
import com.aliyun.odps.*;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TableTunnel;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

public final class OdpsUtil {
    private static final Logger LOG = LoggerFactory.getLogger(OdpsUtil.class);

    public static int MAX_RETRY_TIME = 10;

    public static void checkNecessaryConfig(Configuration originalConfig) {
        originalConfig.getNecessaryValue(Key.ODPS_SERVER, OdpsReaderErrorCode.REQUIRED_VALUE);

        originalConfig.getNecessaryValue(Key.PROJECT, OdpsReaderErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.TABLE, OdpsReaderErrorCode.REQUIRED_VALUE);

        if (null == originalConfig.getList(Key.COLUMN) ||
                originalConfig.getList(Key.COLUMN, String.class).isEmpty()) {
            throw DataXException.asDataXException(OdpsReaderErrorCode.REQUIRED_VALUE, "datax获取不到源表的列信息， 由于您未配置读取源头表的列信息. datax无法知道该抽取表的哪些字段的数据 " +
                    "正确的配置方式是给 column 配置上您需要读取的列名称,用英文逗号分隔.");
        }

    }

    public static void dealMaxRetryTime(Configuration originalConfig) {
        int maxRetryTime = originalConfig.getInt(Key.MAX_RETRY_TIME,
                OdpsUtil.MAX_RETRY_TIME);
        if (maxRetryTime < 1 || maxRetryTime > OdpsUtil.MAX_RETRY_TIME) {
            throw DataXException.asDataXException(OdpsReaderErrorCode.ILLEGAL_VALUE, "您所配置的maxRetryTime 值错误. 该值不能小于1, 且不能大于 " + OdpsUtil.MAX_RETRY_TIME +
                    ". 推荐的配置方式是给maxRetryTime 配置1-11之间的某个值. 请您检查配置并做出相应修改.");
        }
        MAX_RETRY_TIME = maxRetryTime;
    }

    public static Odps initOdps(Configuration originalConfig) {
        String odpsServer = originalConfig.getString(Key.ODPS_SERVER);

        String accessId = originalConfig.getString(Key.ACCESS_ID);
        String accessKey = originalConfig.getString(Key.ACCESS_KEY);
        String project = originalConfig.getString(Key.PROJECT);

        String packageAuthorizedProject = originalConfig.getString(Key.PACKAGE_AUTHORIZED_PROJECT);

        String defaultProject;
        if(StringUtils.isBlank(packageAuthorizedProject)) {
            defaultProject = project;
        } else {
            defaultProject = packageAuthorizedProject;
        }

        String accountType = originalConfig.getString(Key.ACCOUNT_TYPE,
                Constant.DEFAULT_ACCOUNT_TYPE);

        Account account = null;
        if (accountType.equalsIgnoreCase(Constant.DEFAULT_ACCOUNT_TYPE)) {
            account = new AliyunAccount(accessId, accessKey);
        } else {
            throw DataXException.asDataXException(OdpsReaderErrorCode.ACCOUNT_TYPE_ERROR,
                    String.format("不支持的账号类型:[%s]. 账号类型目前仅支持aliyun, taobao.", accountType));
        }

        Odps odps = new Odps(account);
        boolean isPreCheck = originalConfig.getBool("dryRun", false);
        if(isPreCheck) {
            odps.getRestClient().setConnectTimeout(3);
            odps.getRestClient().setReadTimeout(3);
            odps.getRestClient().setRetryTimes(2);
        }
        odps.setDefaultProject(defaultProject);
        odps.setEndpoint(odpsServer);

        return odps;
    }

    public static Table getTable(Odps odps, String projectName, String tableName) {
        final Table table = odps.tables().get(projectName, tableName);
        try {
            //通过这种方式检查表是否存在，失败重试。重试策略：每秒钟重试一次，最大重试3次
            return RetryUtil.executeWithRetry(new Callable<Table>() {
                @Override
                public Table call() throws Exception {
                    table.reload();
                    return table;
                }
            }, 3, 1000, false);
        } catch (Exception e) {
            throwDataXExceptionWhenReloadTable(e, tableName);
        }
        return table;
    }

    public static boolean isPartitionedTable(Table table) {
        return getPartitionDepth(table) > 0;
    }

    public static int getPartitionDepth(Table table) {
        TableSchema tableSchema = table.getSchema();

        return tableSchema.getPartitionColumns().size();
    }

    public static List<String> getTableAllPartitions(Table table) {
        List<Partition> tableAllPartitions = table.getPartitions();

        List<String> retPartitions = new ArrayList<String>();

        if (null != tableAllPartitions) {
            for (Partition partition : tableAllPartitions) {
                retPartitions.add(partition.getPartitionSpec().toString());
            }
        }

        return retPartitions;
    }

    public static List<Column> getTableAllColumns(Table table) {
        TableSchema tableSchema = table.getSchema();
        return tableSchema.getColumns();
    }


    public static List<String> getTableOriginalColumnNameList(
            List<Column> columns) {
        List<String> tableOriginalColumnNameList = new ArrayList<String>();

        for (Column column : columns) {
            tableOriginalColumnNameList.add(column.getName());
        }

        return tableOriginalColumnNameList;
    }

    public static String formatPartition(String partition) {
        if (StringUtils.isBlank(partition)) {
            throw DataXException.asDataXException(OdpsReaderErrorCode.ILLEGAL_VALUE,
                    "您所配置的分区不能为空白.");
        } else {
            return partition.trim().replaceAll(" *= *", "=")
                    .replaceAll(" */ *", ",").replaceAll(" *, *", ",")
                    .replaceAll("'", "");
        }
    }

    public static List<String> formatPartitions(List<String> partitions) {
        if (null == partitions || partitions.isEmpty()) {
            return Collections.emptyList();
        } else {
            List<String> formattedPartitions = new ArrayList<String>();
            for (String partition : partitions) {
                formattedPartitions.add(formatPartition(partition));

            }
            return formattedPartitions;
        }
    }

    public static List<Pair<String, ColumnType>> parseColumns(
            List<String> allNormalColumns, List<String> allPartitionColumns,
            List<String> userConfiguredColumns) {
        List<Pair<String, ColumnType>> parsededColumns = new ArrayList<Pair<String, ColumnType>>();
        // warn: upper & lower case
        for (String column : userConfiguredColumns) {
            MutablePair<String, ColumnType> pair = new MutablePair<String, ColumnType>();
            
            // if constant column
            if (OdpsUtil.checkIfConstantColumn(column)) {
                // remove first and last '
                pair.setLeft(column.substring(1, column.length() - 1));
                pair.setRight(ColumnType.CONSTANT);
                parsededColumns.add(pair);
                continue;
            }

            // if normal column, warn: in o d p s normal columns can not
            // repeated in partitioning columns
            int index = OdpsUtil.indexOfIgnoreCase(allNormalColumns, column);
            if (0 <= index) {
                pair.setLeft(allNormalColumns.get(index));
                pair.setRight(ColumnType.NORMAL);
                parsededColumns.add(pair);
                continue;
            }

            // if partition column
            index = OdpsUtil.indexOfIgnoreCase(allPartitionColumns, column);
            if (0 <= index) {
                pair.setLeft(allPartitionColumns.get(index));
                pair.setRight(ColumnType.PARTITION);
                parsededColumns.add(pair);
                continue;
            }
            // not exist column
            throw DataXException.asDataXException(
                    OdpsReaderErrorCode.ILLEGAL_VALUE,
                    String.format("源头表的列配置错误. 您所配置的列 [%s] 不存在.", column));

        }
        return parsededColumns;
    }
    
    private static int indexOfIgnoreCase(List<String> columnCollection,
            String column) {
        int index = -1;
        for (int i = 0; i < columnCollection.size(); i++) {
            if (columnCollection.get(i).equalsIgnoreCase(column)) {
                index = i;
                break;
            }
        }
        return index;
    }

    public static boolean checkIfConstantColumn(String column) {
        if (column.length() >= 2 && column.startsWith(Constant.COLUMN_CONSTANT_FLAG) &&
                column.endsWith(Constant.COLUMN_CONSTANT_FLAG)) {
            return true;
        } else {
            return false;
        }
    }

    public static TableTunnel.DownloadSession createMasterSessionForNonPartitionedTable(Odps odps, String tunnelServer,
                                                                                        final String projectName, final String tableName) {

        final TableTunnel tunnel = new TableTunnel(odps);
        if (StringUtils.isNoneBlank(tunnelServer)) {
            tunnel.setEndpoint(tunnelServer);
        }

        try {
            return RetryUtil.executeWithRetry(new Callable<TableTunnel.DownloadSession>() {
                @Override
                public TableTunnel.DownloadSession call() throws Exception {
                    return tunnel.createDownloadSession(
                            projectName, tableName);
                }
            }, MAX_RETRY_TIME, 1000, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsReaderErrorCode.CREATE_DOWNLOADSESSION_FAIL, e);
        }
    }

    public static TableTunnel.DownloadSession getSlaveSessionForNonPartitionedTable(Odps odps, final String sessionId,
                                                                                    String tunnelServer, final String projectName, final String tableName) {

        final TableTunnel tunnel = new TableTunnel(odps);
        if (StringUtils.isNoneBlank(tunnelServer)) {
            tunnel.setEndpoint(tunnelServer);
        }

        try {
            return RetryUtil.executeWithRetry(new Callable<TableTunnel.DownloadSession>() {
                @Override
                public TableTunnel.DownloadSession call() throws Exception {
                    return tunnel.getDownloadSession(
                            projectName, tableName, sessionId);
                }
            }, MAX_RETRY_TIME ,1000, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsReaderErrorCode.GET_DOWNLOADSESSION_FAIL, e);
        }
    }

    public static TableTunnel.DownloadSession createMasterSessionForPartitionedTable(Odps odps, String tunnelServer,
                                                                                     final String projectName, final String tableName, String partition) {

        final TableTunnel tunnel = new TableTunnel(odps);
        if (StringUtils.isNoneBlank(tunnelServer)) {
            tunnel.setEndpoint(tunnelServer);
        }

        final PartitionSpec partitionSpec = new PartitionSpec(partition);

        try {
            return RetryUtil.executeWithRetry(new Callable<TableTunnel.DownloadSession>() {
                @Override
                public TableTunnel.DownloadSession call() throws Exception {
                    return tunnel.createDownloadSession(
                            projectName, tableName, partitionSpec);
                }
            }, MAX_RETRY_TIME, 1000, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsReaderErrorCode.CREATE_DOWNLOADSESSION_FAIL, e);
        }
    }

    public static TableTunnel.DownloadSession getSlaveSessionForPartitionedTable(Odps odps, final String sessionId,
                                                                                 String tunnelServer, final String projectName, final String tableName, String partition) {

        final TableTunnel tunnel = new TableTunnel(odps);
        if (StringUtils.isNoneBlank(tunnelServer)) {
            tunnel.setEndpoint(tunnelServer);
        }

        final PartitionSpec partitionSpec = new PartitionSpec(partition);
        try {
            return RetryUtil.executeWithRetry(new Callable<TableTunnel.DownloadSession>() {
                @Override
                public TableTunnel.DownloadSession call() throws Exception {
                    return tunnel.getDownloadSession(
                            projectName, tableName, partitionSpec, sessionId);
                }
            }, MAX_RETRY_TIME, 1000, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsReaderErrorCode.GET_DOWNLOADSESSION_FAIL, e);
        }
    }



    public static RecordReader getRecordReader(final TableTunnel.DownloadSession downloadSession, final long start, final long count,
                                                     final boolean isCompress) {
        try {
            return RetryUtil.executeWithRetry(new Callable<RecordReader>() {
                @Override
                public RecordReader call() throws Exception {
                    return downloadSession.openRecordReader(start, count, isCompress);
                }
            }, MAX_RETRY_TIME, 1000, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsReaderErrorCode.OPEN_RECORD_READER_FAILED,
                    "open RecordReader失败. 请联系 ODPS 管理员处理.", e);
        }
    }

    /**
     * table.reload() 方法抛出的 odps 异常 转化为更清晰的 datax 异常 抛出
     */
    public static void throwDataXExceptionWhenReloadTable(Exception e, String tableName) {
        if(e.getMessage() != null) {
            if(e.getMessage().contains(OdpsExceptionMsg.ODPS_PROJECT_NOT_FOUNT)) {
                throw DataXException.asDataXException(OdpsReaderErrorCode.ODPS_PROJECT_NOT_FOUNT,
                        String.format("加载 ODPS 源头表:%s 失败. " +
                                "请检查您配置的 ODPS 源头表的 [project] 是否正确.", tableName), e);
            } else if(e.getMessage().contains(OdpsExceptionMsg.ODPS_TABLE_NOT_FOUNT)) {
                throw DataXException.asDataXException(OdpsReaderErrorCode.ODPS_TABLE_NOT_FOUNT,
                        String.format("加载 ODPS 源头表:%s 失败. " +
                                "请检查您配置的 ODPS 源头表的 [table] 是否正确.", tableName), e);
            } else if(e.getMessage().contains(OdpsExceptionMsg.ODPS_ACCESS_KEY_ID_NOT_FOUND)) {
                throw DataXException.asDataXException(OdpsReaderErrorCode.ODPS_ACCESS_KEY_ID_NOT_FOUND,
                        String.format("加载 ODPS 源头表:%s 失败. " +
                                "请检查您配置的 ODPS 源头表的 [accessId] [accessKey]是否正确.", tableName), e);
            } else if(e.getMessage().contains(OdpsExceptionMsg.ODPS_ACCESS_KEY_INVALID)) {
                throw DataXException.asDataXException(OdpsReaderErrorCode.ODPS_ACCESS_KEY_INVALID,
                        String.format("加载 ODPS 源头表:%s 失败. " +
                                "请检查您配置的 ODPS 源头表的 [accessKey] 是否正确.", tableName), e);
            } else if(e.getMessage().contains(OdpsExceptionMsg.ODPS_ACCESS_DENY)) {
                throw DataXException.asDataXException(OdpsReaderErrorCode.ODPS_ACCESS_DENY,
                        String.format("加载 ODPS 源头表:%s 失败. " +
                                "请检查您配置的 ODPS 源头表的 [accessId] [accessKey] [project]是否匹配.", tableName), e);
            }
        }
        throw DataXException.asDataXException(OdpsReaderErrorCode.ILLEGAL_VALUE,
                String.format("加载 ODPS 源头表:%s 失败. " +
                        "请检查您配置的 ODPS 源头表的 project,table,accessId,accessKey,odpsServer等值.", tableName), e);
    }
}
