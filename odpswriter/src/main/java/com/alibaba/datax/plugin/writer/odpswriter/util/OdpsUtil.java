package com.alibaba.datax.plugin.writer.odpswriter.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.writer.odpswriter.Constant;
import com.alibaba.datax.plugin.writer.odpswriter.Key;

import com.alibaba.datax.plugin.writer.odpswriter.OdpsWriterErrorCode;
import com.aliyun.odps.*;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;

import com.aliyun.odps.tunnel.io.ProtobufRecordPack;
import com.aliyun.odps.tunnel.io.TunnelRecordWriter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;

public class OdpsUtil {
    private static final Logger LOG = LoggerFactory.getLogger(OdpsUtil.class);

    public static int MAX_RETRY_TIME = 10;

    public static void checkNecessaryConfig(Configuration originalConfig) {
        originalConfig.getNecessaryValue(Key.ODPS_SERVER,
                OdpsWriterErrorCode.REQUIRED_VALUE);

        originalConfig.getNecessaryValue(Key.PROJECT,
                OdpsWriterErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.TABLE,
                OdpsWriterErrorCode.REQUIRED_VALUE);

        if (null == originalConfig.getList(Key.COLUMN) ||
                originalConfig.getList(Key.COLUMN, String.class).isEmpty()) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.REQUIRED_VALUE, "您未配置写入 ODPS 目的表的列信息. " +
                    "正确的配置方式是给datax的 column 项配置上您需要读取的列名称,用英文逗号分隔 例如:  \"column\": [\"id\",\"name\"].");
        }

        // getBool 内部要求，值只能为 true,false 的字符串（大小写不敏感），其他一律报错，不再有默认配置
        Boolean truncate = originalConfig.getBool(Key.TRUNCATE);
        if (null == truncate) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.REQUIRED_VALUE, "[truncate]是必填配置项, 意思是写入 ODPS 目的表前是否清空表/分区. " +
                    "请您增加 truncate 的配置，根据业务需要选择上true 或者 false.");
        }
    }

    public static void dealMaxRetryTime(Configuration originalConfig) {
        int maxRetryTime = originalConfig.getInt(Key.MAX_RETRY_TIME,
                OdpsUtil.MAX_RETRY_TIME);
        if (maxRetryTime < 1 || maxRetryTime > OdpsUtil.MAX_RETRY_TIME) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.ILLEGAL_VALUE, "您所配置的maxRetryTime 值错误. 该值不能小于1, 且不能大于 " + OdpsUtil.MAX_RETRY_TIME +
                    ". 推荐的配置方式是给maxRetryTime 配置1-11之间的某个值. 请您检查配置并做出相应修改.");
        }
        MAX_RETRY_TIME = maxRetryTime;
    }

    public static String formatPartition(String partitionString) {
        if (null == partitionString) {
            return null;
        }

        return partitionString.trim().replaceAll(" *= *", "=").replaceAll(" */ *", ",")
                .replaceAll(" *, *", ",").replaceAll("'", "");
    }


    public static Odps initOdpsProject(Configuration originalConfig) {
        String accountType = originalConfig.getString(Key.ACCOUNT_TYPE);
        String accessId = originalConfig.getString(Key.ACCESS_ID);
        String accessKey = originalConfig.getString(Key.ACCESS_KEY);

        String odpsServer = originalConfig.getString(Key.ODPS_SERVER);
        String project = originalConfig.getString(Key.PROJECT);

        Account account;
        if (accountType.equalsIgnoreCase(Constant.DEFAULT_ACCOUNT_TYPE)) {
            account = new AliyunAccount(accessId, accessKey);
        } else {
            throw DataXException.asDataXException(OdpsWriterErrorCode.ACCOUNT_TYPE_ERROR,
                    String.format("不支持的账号类型:[%s]. 账号类型目前仅支持aliyun, taobao.", accountType));
        }

        Odps odps = new Odps(account);
        boolean isPreCheck = originalConfig.getBool("dryRun", false);
        if(isPreCheck) {
            odps.getRestClient().setConnectTimeout(3);
            odps.getRestClient().setReadTimeout(3);
            odps.getRestClient().setRetryTimes(2);
        }
        odps.setDefaultProject(project);
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

    public static List<String> listOdpsPartitions(Table table) {
        List<String> parts = new ArrayList<String>();
        try {
            List<Partition> partitions = table.getPartitions();
            for(Partition partition : partitions) {
                parts.add(partition.getPartitionSpec().toString());
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.GET_PARTITION_FAIL, String.format("获取 ODPS 目的表:%s 的所有分区失败. 请联系 ODPS 管理员处理.",
                    table.getName()), e);
        }
        return parts;
    }

    public static boolean isPartitionedTable(Table table) {
        //必须要是非分区表才能 truncate 整个表
        List<Column> partitionKeys;
        try {
            partitionKeys = table.getSchema().getPartitionColumns();
            if (null != partitionKeys && !partitionKeys.isEmpty()) {
                return true;
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.CHECK_IF_PARTITIONED_TABLE_FAILED,
                    String.format("检查 ODPS 目的表:%s 是否为分区表失败, 请联系 ODPS 管理员处理.", table.getName()), e);
        }
        return false;
    }


    public static void truncateNonPartitionedTable(Odps odps, Table tab) {
        String truncateNonPartitionedTableSql = "truncate table " + tab.getName() + ";";

        try {
            runSqlTaskWithRetry(odps, truncateNonPartitionedTableSql, MAX_RETRY_TIME, 1000, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.TABLE_TRUNCATE_ERROR,
                    String.format(" 清空 ODPS 目的表:%s 失败, 请联系 ODPS 管理员处理.", tab.getName()), e);
        }
    }

    public static void truncatePartition(Odps odps, Table table, String partition) {
        if (isPartitionExist(table, partition)) {
            dropPart(odps, table, partition);
        }
        addPart(odps, table, partition);
    }

    private static boolean isPartitionExist(Table table, String partition) {
        // check if exist partition 返回值不为 null
        List<String> odpsParts = OdpsUtil.listOdpsPartitions(table);

        int j = 0;
        for (; j < odpsParts.size(); j++) {
            if (odpsParts.get(j).replaceAll("'", "").equals(partition)) {
                break;
            }
        }

        return j != odpsParts.size();
    }

    public static void addPart(Odps odps, Table table, String partition) {
        String partSpec = getPartSpec(partition);
        // add if not exists partition
        StringBuilder addPart = new StringBuilder();
        addPart.append("alter table ").append(table.getName()).append(" add IF NOT EXISTS partition(")
                .append(partSpec).append(");");
        try {
            runSqlTaskWithRetry(odps, addPart.toString(), MAX_RETRY_TIME, 1000, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.ADD_PARTITION_FAILED,
                    String.format("添加 ODPS 目的表的分区失败. 错误发生在添加 ODPS 的项目:%s 的表:%s 的分区:%s. 请联系 ODPS 管理员处理.",
                            table.getProject(), table.getName(), partition), e);
        }
    }


    public static TableTunnel.UploadSession createMasterTunnelUpload(final TableTunnel tunnel, final String projectName,
                                                  final String tableName, final String partition) {
        if(StringUtils.isBlank(partition)) {
            try {
                return RetryUtil.executeWithRetry(new Callable<TableTunnel.UploadSession>() {
                    @Override
                    public TableTunnel.UploadSession call() throws Exception {
                        return tunnel.createUploadSession(projectName, tableName);
                    }
                }, MAX_RETRY_TIME, 1000L, true);
            } catch (Exception e) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.CREATE_MASTER_UPLOAD_FAIL,
                        "创建TunnelUpload失败. 请联系 ODPS 管理员处理.", e);
            }
        } else {
            final PartitionSpec partitionSpec = new PartitionSpec(partition);
            try {
                return RetryUtil.executeWithRetry(new Callable<TableTunnel.UploadSession>() {
                    @Override
                    public TableTunnel.UploadSession call() throws Exception {
                        return tunnel.createUploadSession(projectName, tableName, partitionSpec);
                    }
                }, MAX_RETRY_TIME, 1000L, true);
            } catch (Exception e) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.CREATE_MASTER_UPLOAD_FAIL,
                        "创建TunnelUpload失败. 请联系 ODPS 管理员处理.", e);
            }
        }
    }

    public static TableTunnel.UploadSession getSlaveTunnelUpload(final TableTunnel tunnel, final String projectName, final String tableName,
                                              final String partition, final String uploadId) {

        if(StringUtils.isBlank(partition)) {
            try {
                return RetryUtil.executeWithRetry(new Callable<TableTunnel.UploadSession>() {
                    @Override
                    public TableTunnel.UploadSession call() throws Exception {
                        return tunnel.getUploadSession(projectName, tableName, uploadId);
                    }
                }, MAX_RETRY_TIME, 1000L, true);

            } catch (Exception e) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.GET_SLAVE_UPLOAD_FAIL,
                        "获取TunnelUpload失败. 请联系 ODPS 管理员处理.", e);
            }
        } else {
            final PartitionSpec partitionSpec = new PartitionSpec(partition);
            try {
                return RetryUtil.executeWithRetry(new Callable<TableTunnel.UploadSession>() {
                    @Override
                    public TableTunnel.UploadSession call() throws Exception {
                        return tunnel.getUploadSession(projectName, tableName, partitionSpec, uploadId);
                    }
                }, MAX_RETRY_TIME, 1000L, true);

            } catch (Exception e) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.GET_SLAVE_UPLOAD_FAIL,
                        "获取TunnelUpload失败. 请联系 ODPS 管理员处理.", e);
            }
        }
    }


    private static void dropPart(Odps odps, Table table, String partition) {
        String partSpec = getPartSpec(partition);
        StringBuilder dropPart = new StringBuilder();
        dropPart.append("alter table ").append(table.getName())
                .append(" drop IF EXISTS partition(").append(partSpec)
                .append(");");
        try {
            runSqlTaskWithRetry(odps, dropPart.toString(), MAX_RETRY_TIME, 1000, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.ADD_PARTITION_FAILED,
                    String.format("Drop  ODPS 目的表分区失败. 错误发生在项目:%s 的表:%s 的分区:%s .请联系 ODPS 管理员处理.",
                            table.getProject(), table.getName(), partition), e);
        }
    }

    private static String getPartSpec(String partition) {
        StringBuilder partSpec = new StringBuilder();
        String[] parts = partition.split(",");
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            String[] kv = part.split("=");
            if (kv.length != 2) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.ILLEGAL_VALUE,
                        String.format("ODPS 目的表自身的 partition:%s 格式不对. 正确的格式形如: pt=1,ds=hangzhou", partition));
            }
            partSpec.append(kv[0]).append("=");
            partSpec.append("'").append(kv[1].replace("'", "")).append("'");
            if (i != parts.length - 1) {
                partSpec.append(",");
            }
        }
        return partSpec.toString();
    }

    /**
     * 该方法只有在 sql 为幂等的才可以使用，且odps抛出异常时候才会进行重试
     *
     * @param odps    odps
     * @param query   执行sql
     * @throws Exception
     */
    public static void runSqlTaskWithRetry(final Odps odps, final String query, int retryTimes,
                                            long sleepTimeInMilliSecond, boolean exponential) throws Exception {
        for(int i = 0; i < retryTimes; i++) {
            try {
                runSqlTask(odps, query);
                return;
            } catch (DataXException e) {
                if (OdpsWriterErrorCode.RUN_SQL_ODPS_EXCEPTION.equals(e.getErrorCode())) {
                    LOG.debug("Exception when calling callable", e);
                    if (i + 1 < retryTimes && sleepTimeInMilliSecond > 0) {
                        LOG.warn(String.format("will do [%s] times retry, current exception=%s", i + 1, e.getMessage()));
                        long timeToSleep;
                        if (exponential) {
                            timeToSleep = sleepTimeInMilliSecond * (long) Math.pow(2, i);
                            if(timeToSleep >= 128 * 1000) {
                                timeToSleep = 128 * 1000;
                            }
                        } else {
                            timeToSleep = sleepTimeInMilliSecond;
                            if(timeToSleep >= 128 * 1000) {
                                timeToSleep = 128 * 1000;
                            }
                        }

                        try {
                            Thread.sleep(timeToSleep);
                        } catch (InterruptedException ignored) {
                        }
                    } else {
                        throw e;
                    }
                } else {
                    throw e;
                }
            } catch (Exception e) {
                throw e;
            }
        }
    }

    public static void runSqlTask(Odps odps, String query) {
        if (StringUtils.isBlank(query)) {
            return;
        }

        String taskName = "datax_odpswriter_trunacte_" + UUID.randomUUID().toString().replace('-', '_');

        LOG.info("Try to start sqlTask:[{}] to run odps sql:[\n{}\n] .", taskName, query);

        //todo:biz_id set (目前ddl先不做)
        Instance instance;
        Instance.TaskStatus status;
        try {
            instance = SQLTask.run(odps, odps.getDefaultProject(), query, taskName, null, null);
            instance.waitForSuccess();
            status = instance.getTaskStatus().get(taskName);
            if (!Instance.TaskStatus.Status.SUCCESS.equals(status.getStatus())) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.RUN_SQL_FAILED,
                        String.format("ODPS 目的表在运行 ODPS SQL失败, 返回值为:%s. 请联系 ODPS 管理员处理. SQL 内容为:[\n%s\n].", instance.getTaskResults().get(taskName),
                                query));
            }
        } catch (DataXException e) {
            throw e;
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.RUN_SQL_ODPS_EXCEPTION,
                    String.format("ODPS 目的表在运行 ODPS SQL 时抛出异常, 请联系 ODPS 管理员处理. SQL 内容为:[\n%s\n].", query), e);
        }
    }

    public static void masterCompleteBlocks(final TableTunnel.UploadSession masterUpload, final Long[] blocks) {
        try {
            RetryUtil.executeWithRetry(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    masterUpload.commit(blocks);
                    return null;
                }
            }, MAX_RETRY_TIME, 1000L, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.COMMIT_BLOCK_FAIL,
                    String.format("ODPS 目的表在提交 block:[\n%s\n] 时失败, uploadId=[%s]. 请联系 ODPS 管理员处理.", StringUtils.join(blocks, ","), masterUpload.getId()), e);
        }
    }

    public static void slaveWriteOneBlock(final TableTunnel.UploadSession slaveUpload, final ProtobufRecordPack protobufRecordPack,
                                          final long blockId, final boolean isCompress) {
        try {
            RetryUtil.executeWithRetry(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    TunnelRecordWriter tunnelRecordWriter = (TunnelRecordWriter)slaveUpload.openRecordWriter(blockId, isCompress);
                    tunnelRecordWriter.write(protobufRecordPack);
                    tunnelRecordWriter.close();
                    return null;
                }
            }, MAX_RETRY_TIME, 1000L, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.WRITER_RECORD_FAIL,
                    String.format("ODPS 目的表写 block:%s 失败， uploadId=[%s]. 请联系 ODPS 管理员处理.", blockId, slaveUpload.getId()), e);
        }

    }

    public static List<Integer> parsePosition(List<String> allColumnList,
                                              List<String> userConfiguredColumns) {
        List<Integer> retList = new ArrayList<Integer>();

        boolean hasColumn;
        for (String col : userConfiguredColumns) {
            hasColumn = false;
            for (int i = 0, len = allColumnList.size(); i < len; i++) {
                if (allColumnList.get(i).equalsIgnoreCase(col)) {
                    retList.add(i);
                    hasColumn = true;
                    break;
                }
            }
            if (!hasColumn) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.COLUMN_NOT_EXIST,
                        String.format("ODPS 目的表的列配置错误. 由于您所配置的列:%s 不存在，会导致datax无法正常插入数据，请检查该列是否存在，如果存在请检查大小写等配置.", col));
            }
        }
        return retList;
    }

    public static List<String> getAllColumns(TableSchema schema) {
        if (null == schema) {
            throw new IllegalArgumentException("parameter schema can not be null.");
        }

        List<String> allColumns = new ArrayList<String>();

        List<Column> columns = schema.getColumns();
        OdpsType type;
        for(Column column: columns) {
            allColumns.add(column.getName());
            type = column.getType();
            if (type == OdpsType.ARRAY || type == OdpsType.MAP) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.UNSUPPORTED_COLUMN_TYPE,
                        String.format("DataX 写入 ODPS 表不支持该字段类型:[%s]. 目前支持抽取的字段类型有：bigint, boolean, datetime, double, string. " +
                                        "您可以选择不抽取 DataX 不支持的字段或者联系 ODPS 管理员寻求帮助.",
                                type));
            }
        }
        return allColumns;
    }

    public static List<OdpsType> getTableOriginalColumnTypeList(TableSchema schema) {
        List<OdpsType> tableOriginalColumnTypeList = new ArrayList<OdpsType>();

        List<Column> columns = schema.getColumns();
        for (Column column : columns) {
            tableOriginalColumnTypeList.add(column.getType());
        }

        return tableOriginalColumnTypeList;
    }

    public static void dealTruncate(Odps odps, Table table, String partition, boolean truncate) {
        boolean isPartitionedTable = OdpsUtil.isPartitionedTable(table);

        if (truncate) {
            //需要 truncate
            if (isPartitionedTable) {
                //分区表
                if (StringUtils.isBlank(partition)) {
                    throw DataXException.asDataXException(OdpsWriterErrorCode.PARTITION_ERROR, String.format("您没有配置分区信息，因为你配置的表是分区表:%s 如果需要进行 truncate 操作，必须指定需要清空的具体分区. 请修改分区配置，格式形如 pt=${bizdate} .",
                            table.getName()));
                } else {
                    LOG.info("Try to truncate partition=[{}] in table=[{}].", partition, table.getName());
                    OdpsUtil.truncatePartition(odps, table, partition);
                }
            } else {
                //非分区表
                if (StringUtils.isNotBlank(partition)) {
                    throw DataXException.asDataXException(OdpsWriterErrorCode.PARTITION_ERROR, String.format("分区信息配置错误，你的ODPS表是非分区表:%s 进行 truncate 操作时不需要指定具体分区值. 请检查您的分区配置，删除该配置项的值.",
                            table.getName()));
                } else {
                    LOG.info("Try to truncate table:[{}].", table.getName());
                    OdpsUtil.truncateNonPartitionedTable(odps, table);
                }
            }
        } else {
            //不需要 truncate
            if (isPartitionedTable) {
                //分区表
                if (StringUtils.isBlank(partition)) {
                    throw DataXException.asDataXException(OdpsWriterErrorCode.PARTITION_ERROR,
                            String.format("您的目的表是分区表，写入分区表:%s 时必须指定具体分区值. 请修改您的分区配置信息，格式形如 格式形如 pt=${bizdate}.", table.getName()));
                } else {
                    boolean isPartitionExists = OdpsUtil.isPartitionExist(table, partition);
                    if (!isPartitionExists) {
                        LOG.info("Try to add partition:[{}] in table:[{}].", partition,
                                table.getName());
                        OdpsUtil.addPart(odps, table, partition);
                    }
                }
            } else {
                //非分区表
                if (StringUtils.isNotBlank(partition)) {
                    throw DataXException.asDataXException(OdpsWriterErrorCode.PARTITION_ERROR,
                            String.format("您的目的表是非分区表，写入非分区表:%s 时不需要指定具体分区值. 请删除分区配置信息", table.getName()));
                }
            }
        }
    }


    /**
     * 检查odpswriter 插件的分区信息
     *
     * @param odps
     * @param table
     * @param partition
     * @param truncate
     */
    public static void preCheckPartition(Odps odps, Table table, String partition, boolean truncate) {
        boolean isPartitionedTable = OdpsUtil.isPartitionedTable(table);

        if (truncate) {
            //需要 truncate
            if (isPartitionedTable) {
                //分区表
                if (StringUtils.isBlank(partition)) {
                    throw DataXException.asDataXException(OdpsWriterErrorCode.PARTITION_ERROR, String.format("您没有配置分区信息，因为你配置的表是分区表:%s 如果需要进行 truncate 操作，必须指定需要清空的具体分区. 请修改分区配置，格式形如 pt=${bizdate} .",
                            table.getName()));
                }
            } else {
                //非分区表
                if (StringUtils.isNotBlank(partition)) {
                    throw DataXException.asDataXException(OdpsWriterErrorCode.PARTITION_ERROR, String.format("分区信息配置错误，你的ODPS表是非分区表:%s 进行 truncate 操作时不需要指定具体分区值. 请检查您的分区配置，删除该配置项的值.",
                            table.getName()));
                }
            }
        } else {
            //不需要 truncate
            if (isPartitionedTable) {
                //分区表
                if (StringUtils.isBlank(partition)) {
                    throw DataXException.asDataXException(OdpsWriterErrorCode.PARTITION_ERROR,
                            String.format("您的目的表是分区表，写入分区表:%s 时必须指定具体分区值. 请修改您的分区配置信息，格式形如 格式形如 pt=${bizdate}.", table.getName()));
                }
            } else {
                //非分区表
                if (StringUtils.isNotBlank(partition)) {
                    throw DataXException.asDataXException(OdpsWriterErrorCode.PARTITION_ERROR,
                            String.format("您的目的表是非分区表，写入非分区表:%s 时不需要指定具体分区值. 请删除分区配置信息", table.getName()));
                }
            }
        }
    }

    /**
     * table.reload() 方法抛出的 odps 异常 转化为更清晰的 datax 异常 抛出
     */
    public static void throwDataXExceptionWhenReloadTable(Exception e, String tableName) {
        if(e.getMessage() != null) {
            if(e.getMessage().contains(OdpsExceptionMsg.ODPS_PROJECT_NOT_FOUNT)) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.ODPS_PROJECT_NOT_FOUNT,
                        String.format("加载 ODPS 目的表:%s 失败. " +
                                "请检查您配置的 ODPS 目的表的 [project] 是否正确.", tableName), e);
            } else if(e.getMessage().contains(OdpsExceptionMsg.ODPS_TABLE_NOT_FOUNT)) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.ODPS_TABLE_NOT_FOUNT,
                        String.format("加载 ODPS 目的表:%s 失败. " +
                                "请检查您配置的 ODPS 目的表的 [table] 是否正确.", tableName), e);
            } else if(e.getMessage().contains(OdpsExceptionMsg.ODPS_ACCESS_KEY_ID_NOT_FOUND)) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.ODPS_ACCESS_KEY_ID_NOT_FOUND,
                        String.format("加载 ODPS 目的表:%s 失败. " +
                                "请检查您配置的 ODPS 目的表的 [accessId] [accessKey]是否正确.", tableName), e);
            } else if(e.getMessage().contains(OdpsExceptionMsg.ODPS_ACCESS_KEY_INVALID)) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.ODPS_ACCESS_KEY_INVALID,
                        String.format("加载 ODPS 目的表:%s 失败. " +
                                "请检查您配置的 ODPS 目的表的 [accessKey] 是否正确.", tableName), e);
            } else if(e.getMessage().contains(OdpsExceptionMsg.ODPS_ACCESS_DENY)) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.ODPS_ACCESS_DENY,
                        String.format("加载 ODPS 目的表:%s 失败. " +
                                "请检查您配置的 ODPS 目的表的 [accessId] [accessKey] [project]是否匹配.", tableName), e);
            }
        }
        throw DataXException.asDataXException(OdpsWriterErrorCode.ILLEGAL_VALUE,
                String.format("加载 ODPS 目的表:%s 失败. " +
                        "请检查您配置的 ODPS 目的表的 project,table,accessId,accessKey,odpsServer等值.", tableName), e);
    }

}
