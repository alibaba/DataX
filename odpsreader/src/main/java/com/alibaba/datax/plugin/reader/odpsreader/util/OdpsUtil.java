package com.alibaba.datax.plugin.reader.odpsreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.DataXCaseEnvUtil;
import com.alibaba.datax.common.util.MessageSource;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.reader.odpsreader.ColumnType;
import com.alibaba.datax.plugin.reader.odpsreader.Constant;
import com.alibaba.datax.plugin.reader.odpsreader.Key;
import com.alibaba.datax.plugin.reader.odpsreader.OdpsReaderErrorCode;
import com.aliyun.odps.*;
import com.aliyun.odps.Column;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.StsAccount;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.type.TypeInfo;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;

public final class OdpsUtil {
    private static final Logger LOG = LoggerFactory.getLogger(OdpsUtil.class);
    private static final MessageSource MESSAGE_SOURCE = MessageSource.loadResourceBundle(OdpsUtil.class);

    public static int MAX_RETRY_TIME = 10;

    public static void checkNecessaryConfig(Configuration originalConfig) {
        originalConfig.getNecessaryValue(Key.ODPS_SERVER, OdpsReaderErrorCode.REQUIRED_VALUE);

        originalConfig.getNecessaryValue(Key.PROJECT, OdpsReaderErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.TABLE, OdpsReaderErrorCode.REQUIRED_VALUE);

        if (null == originalConfig.getList(Key.COLUMN) ||
                originalConfig.getList(Key.COLUMN, String.class).isEmpty()) {
            throw DataXException.asDataXException(OdpsReaderErrorCode.REQUIRED_VALUE,
                    MESSAGE_SOURCE.message("odpsutil.1"));
        }

    }

    public static void dealMaxRetryTime(Configuration originalConfig) {
        int maxRetryTime = originalConfig.getInt(Key.MAX_RETRY_TIME,
                OdpsUtil.MAX_RETRY_TIME);
        if (maxRetryTime < 1 || maxRetryTime > OdpsUtil.MAX_RETRY_TIME) {
            throw DataXException.asDataXException(OdpsReaderErrorCode.ILLEGAL_VALUE,
                    MESSAGE_SOURCE.message("odpsutil.2", OdpsUtil.MAX_RETRY_TIME));
        }
        MAX_RETRY_TIME = maxRetryTime;
    }

    public static Odps initOdps(Configuration originalConfig) {
        String odpsServer = originalConfig.getString(Key.ODPS_SERVER);

        String accessId = originalConfig.getString(Key.ACCESS_ID);
        String accessKey = originalConfig.getString(Key.ACCESS_KEY);
        String project = originalConfig.getString(Key.PROJECT);
        String securityToken = originalConfig.getString(Key.SECURITY_TOKEN);

        String packageAuthorizedProject = originalConfig.getString(Key.PACKAGE_AUTHORIZED_PROJECT);

        String defaultProject;
        if (StringUtils.isBlank(packageAuthorizedProject)) {
            defaultProject = project;
        } else {
            defaultProject = packageAuthorizedProject;
        }

        String accountType = originalConfig.getString(Key.ACCOUNT_TYPE,
                Constant.DEFAULT_ACCOUNT_TYPE);

        Account account = null;
        if (accountType.equalsIgnoreCase(Constant.DEFAULT_ACCOUNT_TYPE)) {
            if (StringUtils.isNotBlank(securityToken)) {
                account = new StsAccount(accessId, accessKey, securityToken);
            } else {
                account = new AliyunAccount(accessId, accessKey);
            }
        } else {
            throw DataXException.asDataXException(OdpsReaderErrorCode.ACCOUNT_TYPE_ERROR,
                    MESSAGE_SOURCE.message("odpsutil.3", accountType));
        }

        Odps odps = new Odps(account);
        boolean isPreCheck = originalConfig.getBool("dryRun", false);
        if (isPreCheck) {
            odps.getRestClient().setConnectTimeout(3);
            odps.getRestClient().setReadTimeout(3);
            odps.getRestClient().setRetryTimes(2);
        }
        odps.setDefaultProject(defaultProject);
        odps.setEndpoint(odpsServer);
        odps.setUserAgent("DATAX");

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
            }, DataXCaseEnvUtil.getRetryTimes(3), DataXCaseEnvUtil.getRetryInterval(1000), DataXCaseEnvUtil.getRetryExponential(false));
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
                    MESSAGE_SOURCE.message("odpsutil.4"));
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

    /**
     * 将用户配置的分区分类成两类:
     * (1) 包含 HINT 的区间过滤;
     * (2) 不包含 HINT 的普通模式
     * @param userConfiguredPartitions
     * @return
     */
    public static UserConfiguredPartitionClassification classifyUserConfiguredPartitions(List<String> userConfiguredPartitions){
        UserConfiguredPartitionClassification userConfiguredPartitionClassification = new UserConfiguredPartitionClassification();

        List<String> userConfiguredHintPartition = new ArrayList<String>();
        List<String> userConfiguredNormalPartition = new ArrayList<String>();
        boolean isIncludeHintPartition = false;
        for (String userConfiguredPartition : userConfiguredPartitions){
            if (StringUtils.isNotBlank(userConfiguredPartition)){
                if (userConfiguredPartition.trim().toLowerCase().startsWith(Constant.PARTITION_FILTER_HINT)) {
                    userConfiguredHintPartition.add(userConfiguredPartition.trim());
                    isIncludeHintPartition = true;
                }else {
                    userConfiguredNormalPartition.add(userConfiguredPartition.trim());
                }
            }
        }
        userConfiguredPartitionClassification.setIncludeHintPartition(isIncludeHintPartition);
        userConfiguredPartitionClassification.setUserConfiguredHintPartition(userConfiguredHintPartition);
        userConfiguredPartitionClassification.setUserConfiguredNormalPartition(userConfiguredNormalPartition);
        return userConfiguredPartitionClassification;
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
                    MESSAGE_SOURCE.message("odpsutil.5", column));

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
            }, DataXCaseEnvUtil.getRetryTimes(MAX_RETRY_TIME), DataXCaseEnvUtil.getRetryInterval(1000), DataXCaseEnvUtil.getRetryExponential(true));
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
            }, DataXCaseEnvUtil.getRetryTimes(MAX_RETRY_TIME), DataXCaseEnvUtil.getRetryInterval(1000), DataXCaseEnvUtil.getRetryExponential(true));
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
            }, DataXCaseEnvUtil.getRetryTimes(MAX_RETRY_TIME), DataXCaseEnvUtil.getRetryInterval(1000), DataXCaseEnvUtil.getRetryExponential(true));
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
            }, DataXCaseEnvUtil.getRetryTimes(MAX_RETRY_TIME), DataXCaseEnvUtil.getRetryInterval(1000), DataXCaseEnvUtil.getRetryExponential(true));
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsReaderErrorCode.GET_DOWNLOADSESSION_FAIL, e);
        }
    }

    /**
     * odpsreader采用的直接读取所有列的downloadSession
     */
    public static RecordReader getRecordReader(final TableTunnel.DownloadSession downloadSession, final long start, final long count,
                                               final boolean isCompress) {
        try {
            return RetryUtil.executeWithRetry(new Callable<RecordReader>() {
                @Override
                public RecordReader call() throws Exception {
                    return downloadSession.openRecordReader(start, count, isCompress);
                }
            }, DataXCaseEnvUtil.getRetryTimes(MAX_RETRY_TIME), DataXCaseEnvUtil.getRetryInterval(1000), DataXCaseEnvUtil.getRetryExponential(true));
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsReaderErrorCode.OPEN_RECORD_READER_FAILED,
                    MESSAGE_SOURCE.message("odpsutil.6"), e);
        }
    }


    /**
     * odpsreader采用的指定读取某些列的downloadSession
     */
    public static RecordReader getRecordReader(final TableTunnel.DownloadSession downloadSession, final long start, final long count,
                                               final boolean isCompress, final List<Column> columns) {
        try {
            return RetryUtil.executeWithRetry(new Callable<RecordReader>() {
                @Override
                public RecordReader call() throws Exception {
                    return downloadSession.openRecordReader(start, count, isCompress, columns);
                }
            }, DataXCaseEnvUtil.getRetryTimes(MAX_RETRY_TIME), DataXCaseEnvUtil.getRetryInterval(1000), DataXCaseEnvUtil.getRetryExponential(true));
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsReaderErrorCode.OPEN_RECORD_READER_FAILED,
                    MESSAGE_SOURCE.message("odpsutil.6"), e);
        }
    }


    /**
     * table.reload() 方法抛出的 odps 异常 转化为更清晰的 datax 异常 抛出
     */
    public static void throwDataXExceptionWhenReloadTable(Exception e, String tableName) {
        if (e.getMessage() != null) {
            if (e.getMessage().contains(OdpsExceptionMsg.ODPS_PROJECT_NOT_FOUNT)) {
                throw DataXException.asDataXException(OdpsReaderErrorCode.ODPS_PROJECT_NOT_FOUNT,
                        MESSAGE_SOURCE.message("odpsutil.7", tableName), e);
            } else if (e.getMessage().contains(OdpsExceptionMsg.ODPS_TABLE_NOT_FOUNT)) {
                throw DataXException.asDataXException(OdpsReaderErrorCode.ODPS_TABLE_NOT_FOUNT,
                        MESSAGE_SOURCE.message("odpsutil.8", tableName), e);
            } else if (e.getMessage().contains(OdpsExceptionMsg.ODPS_ACCESS_KEY_ID_NOT_FOUND)) {
                throw DataXException.asDataXException(OdpsReaderErrorCode.ODPS_ACCESS_KEY_ID_NOT_FOUND,
                        MESSAGE_SOURCE.message("odpsutil.9", tableName), e);
            } else if (e.getMessage().contains(OdpsExceptionMsg.ODPS_ACCESS_KEY_INVALID)) {
                throw DataXException.asDataXException(OdpsReaderErrorCode.ODPS_ACCESS_KEY_INVALID,
                        MESSAGE_SOURCE.message("odpsutil.10", tableName), e);
            } else if (e.getMessage().contains(OdpsExceptionMsg.ODPS_ACCESS_DENY)) {
                throw DataXException.asDataXException(OdpsReaderErrorCode.ODPS_ACCESS_DENY,
                        MESSAGE_SOURCE.message("odpsutil.11", tableName), e);
            }
        }
        throw DataXException.asDataXException(OdpsReaderErrorCode.ILLEGAL_VALUE,
                MESSAGE_SOURCE.message("odpsutil.12", tableName), e);
    }

    public static List<Column> getNormalColumns(List<Pair<String, ColumnType>> parsedColumns,
                                                Map<String, TypeInfo> columnTypeMap) {
        List<Column> userConfigNormalColumns = new ArrayList<Column>();
        Set<String> columnNameSet = new HashSet<String>();
        for (Pair<String, ColumnType> columnInfo : parsedColumns) {
            if (columnInfo.getValue() == ColumnType.NORMAL) {
                String columnName = columnInfo.getKey();
                if (!columnNameSet.contains(columnName)) {
                    Column column = new Column(columnName, columnTypeMap.get(columnName));
                    userConfigNormalColumns.add(column);
                    columnNameSet.add(columnName);
                }
            }
        }
        return userConfigNormalColumns;
    }

    /**
     * 执行odps preSql和postSql
     *
     * @param odps: odps client
     * @param sql : 要执行的odps sql语句, 因为会有重试, 所以sql 必须为幂等的
     * @param tag : "preSql" or "postSql"
     */
    public static void runSqlTaskWithRetry(final Odps odps, final String sql, final String tag){
        //重试次数
        int retryTimes = 10;
        //重试间隔(ms)
        long sleepTimeInMilliSecond = 1000L;
        try {
            RetryUtil.executeWithRetry(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    long beginTime = System.currentTimeMillis();

                    runSqlTask(odps, sql, tag);

                    long endIime = System.currentTimeMillis();
                    LOG.info(String.format("exectue odps sql: %s finished, cost time : %s ms",
                        sql, (endIime - beginTime)));
                    return null;
                }
            }, DataXCaseEnvUtil.getRetryTimes(retryTimes), DataXCaseEnvUtil.getRetryInterval(sleepTimeInMilliSecond), DataXCaseEnvUtil.getRetryExponential(true));
        } catch (Exception e) {
            String errMessage = String.format("Retry %s times to exectue sql :[%s] failed! Exception: %s",
                retryTimes, e.getMessage());
            throw DataXException.asDataXException(OdpsReaderErrorCode.RUN_SQL_ODPS_EXCEPTION, errMessage, e);
        }
    }

    public static void runSqlTask(Odps odps, String sql, String tag) {
        if (StringUtils.isBlank(sql)) {
            return;
        }

        String taskName = String.format("datax_odpsreader_%s_%s", tag, UUID.randomUUID().toString().replace('-', '_'));

        LOG.info("Try to start sqlTask:[{}] to run odps sql:[\n{}\n] .", taskName, sql);

        Instance instance;
        Instance.TaskStatus status;
        try {
            Map<String, String> hints = new HashMap<String, String>();
            hints.put("odps.sql.submit.mode", "script");
            instance = SQLTask.run(odps, odps.getDefaultProject(), sql, taskName, hints, null);
            instance.waitForSuccess();
            status = instance.getTaskStatus().get(taskName);
            if (!Instance.TaskStatus.Status.SUCCESS.equals(status.getStatus())) {
                throw DataXException.asDataXException(OdpsReaderErrorCode.RUN_SQL_FAILED,
                    MESSAGE_SOURCE.message("odpsutil.13", sql));
            }
        } catch (DataXException e) {
            throw e;
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsReaderErrorCode.RUN_SQL_ODPS_EXCEPTION,
                MESSAGE_SOURCE.message("odpsutil.14", sql), e);
        }
    }
}
