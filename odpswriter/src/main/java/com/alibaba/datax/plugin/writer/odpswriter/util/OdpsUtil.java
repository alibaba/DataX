package com.alibaba.datax.plugin.writer.odpswriter.util;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.MessageSource;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.writer.odpswriter.*;
import com.aliyun.odps.*;
import com.aliyun.odps.Column;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.ProtobufRecordPack;
import com.aliyun.odps.tunnel.io.TunnelRecordWriter;
import com.aliyun.odps.type.TypeInfo;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;

public class OdpsUtil {
    private static final Logger LOG = LoggerFactory.getLogger(OdpsUtil.class);
    private static final MessageSource MESSAGE_SOURCE = MessageSource.loadResourceBundle(OdpsUtil.class);

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
            throw DataXException.asDataXException(OdpsWriterErrorCode.REQUIRED_VALUE, MESSAGE_SOURCE.message("odpsutil.1"));
        }

        // getBool 内部要求，值只能为 true,false 的字符串（大小写不敏感），其他一律报错，不再有默认配置
        // 如果是动态分区写入，不进行truncate
        Boolean truncate = originalConfig.getBool(Key.TRUNCATE);
        if (null == truncate) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.REQUIRED_VALUE, MESSAGE_SOURCE.message("odpsutil.2"));
        }
    }

    public static void dealMaxRetryTime(Configuration originalConfig) {
        int maxRetryTime = originalConfig.getInt(Key.MAX_RETRY_TIME,
                OdpsUtil.MAX_RETRY_TIME);
        if (maxRetryTime < 1 || maxRetryTime > OdpsUtil.MAX_RETRY_TIME) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.ILLEGAL_VALUE, MESSAGE_SOURCE.message("odpsutil.3", OdpsUtil.MAX_RETRY_TIME));
        }
        MAX_RETRY_TIME = maxRetryTime;
    }

    public static String formatPartition(String partitionString, Boolean printLog) {
        if (null == partitionString) {
            return null;
        }
        String parsedPartition = partitionString.trim().replaceAll(" *= *", "=").replaceAll(" */ *", ",")
                .replaceAll(" *, *", ",").replaceAll("'", "");
        if (printLog) {
            LOG.info("format partition with rules:  remove all space;  remove all ';  replace / to ,");
            LOG.info("original partiton {}  parsed partition {}", partitionString, parsedPartition);
        }
        return parsedPartition;
    }


    public static Odps initOdpsProject(Configuration originalConfig) {
        String accessId = originalConfig.getString(Key.ACCESS_ID);
        String accessKey = originalConfig.getString(Key.ACCESS_KEY);

        String odpsServer = originalConfig.getString(Key.ODPS_SERVER);
        String project = originalConfig.getString(Key.PROJECT);
        String securityToken = originalConfig.getString(Key.SECURITY_TOKEN);

        Account account;
        if (StringUtils.isNotBlank(securityToken)) {
            account = new com.aliyun.odps.account.StsAccount(accessId, accessKey, securityToken);
        } else {
            account = new AliyunAccount(accessId, accessKey);
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
            throw DataXException.asDataXException(OdpsWriterErrorCode.GET_PARTITION_FAIL, MESSAGE_SOURCE.message("odpsutil.5", table.getName()), e);
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
                    MESSAGE_SOURCE.message("odpsutil.6", table.getName()), e);
        }
        return false;
    }


    public static void truncateNonPartitionedTable(Odps odps, Table tab) {
        truncateNonPartitionedTable(odps, tab.getName());
    }

    public static void truncateNonPartitionedTable(Odps odps, String tableName) {
        String truncateNonPartitionedTableSql = "truncate table " + tableName + ";";

        try {
            LOG.info("truncate non partitioned table with sql: {}", truncateNonPartitionedTableSql);
            runSqlTaskWithRetry(odps, truncateNonPartitionedTableSql, MAX_RETRY_TIME, 1000, true, "truncate", null);
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.TABLE_TRUNCATE_ERROR,
                    MESSAGE_SOURCE.message("odpsutil.7", tableName), e);
        }
    }

    public static void truncatePartition(Odps odps, Table table, String partition) {
        if (isPartitionExist(table, partition)) {
            LOG.info("partition {} is already exist, truncate it to clean old data", partition);
            dropPart(odps, table, partition);
        }
        LOG.info("begin to add partition {}", partition);
        addPart(odps, table, partition);
    }

    private static boolean isPartitionExist(Table table, String partition) {
        // check if exist partition 返回值不为 null
        List<String> odpsParts = OdpsUtil.listOdpsPartitions(table);
        int j = 0;
        for (; j < odpsParts.size(); j++) {
            if (odpsParts.get(j).replaceAll("'", "").equals(partition)) {
                LOG.info("found a partiton {} equals to (ignore ' if contains) configured partiton {}",
                        odpsParts.get(j), partition);
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
            Map<String, String> hints = new HashMap<String, String>();
            //开启ODPS SQL TYPE2.0类型
            hints.put("odps.sql.type.system.odps2", "true");
            LOG.info("add partition with sql: {}", addPart.toString());
            runSqlTaskWithRetry(odps, addPart.toString(), MAX_RETRY_TIME, 1000, true, "addPart", hints);
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.ADD_PARTITION_FAILED,
                    MESSAGE_SOURCE.message("odpsutil.8", table.getProject(), table.getName(), partition), e);
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
                        MESSAGE_SOURCE.message("odpsutil.9"), e);
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
                        MESSAGE_SOURCE.message("odpsutil.10"), e);
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
                        MESSAGE_SOURCE.message("odpsutil.11"), e);
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
                        MESSAGE_SOURCE.message("odpsutil.12"), e);
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
            Map<String, String> hints = new HashMap<String, String>();
            //开启ODPS SQL TYPE2.0类型
            hints.put("odps.sql.type.system.odps2", "true");
            LOG.info("drop partition with sql: {}", dropPart.toString());
            runSqlTaskWithRetry(odps, dropPart.toString(), MAX_RETRY_TIME, 1000, true, "truncate", hints);
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.ADD_PARTITION_FAILED,
                    MESSAGE_SOURCE.message("odpsutil.13", table.getProject(), table.getName(), partition), e);
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
                        MESSAGE_SOURCE.message("odpsutil.14", partition));
            }
            partSpec.append(kv[0]).append("=");
            partSpec.append("'").append(kv[1].replace("'", "")).append("'");
            if (i != parts.length - 1) {
                partSpec.append(",");
            }
        }
        return partSpec.toString();
    }

    public static Instance runSqlTaskWithRetry(final Odps odps, final String sql, String tag) {
        try {
            long beginTime = System.currentTimeMillis();

            Instance instance = runSqlTaskWithRetry(odps, sql, MAX_RETRY_TIME, 1000, true, tag, null);

            long endIime = System.currentTimeMillis();
            LOG.info(String.format("exectue odps sql: %s finished, cost time : %s ms",
                sql, (endIime - beginTime)));
            return instance;
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.RUN_SQL_ODPS_EXCEPTION,
                MESSAGE_SOURCE.message("odpsutil.16", sql), e);
        }
    }

    public static ResultSet getSqlTaskRecordsWithRetry(final Odps odps, final String sql, String tag) {
        Instance instance = runSqlTaskWithRetry(odps, sql, tag);
        if (instance == null) {
            LOG.error("can not get odps instance from sql {}", sql);
            throw DataXException.asDataXException(OdpsWriterErrorCode.RUN_SQL_ODPS_EXCEPTION,
                    MESSAGE_SOURCE.message("odpsutil.16", sql));
        }
        try {
            return SQLTask.getResultSet(instance, instance.getTaskNames().iterator().next());
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.RUN_SQL_ODPS_EXCEPTION,
                    MESSAGE_SOURCE.message("odpsutil.16", sql), e);
        }
    }


    /**
     * 该方法只有在 sql 为幂等的才可以使用，且odps抛出异常时候才会进行重试
     *
     * @param odps    odps
     * @param query   执行sql
     * @throws Exception
     */
    public static Instance runSqlTaskWithRetry(final Odps odps, final String query, int retryTimes,
                                            long sleepTimeInMilliSecond, boolean exponential, String tag,
                                            Map<String, String> hints) throws Exception {
        for(int i = 0; i < retryTimes; i++) {
            try {
                return runSqlTask(odps, query, tag, hints);
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
        return null;
    }

    public static Instance runSqlTask(Odps odps, String query, String tag, Map<String, String> hints) {
        if (StringUtils.isBlank(query)) {
            return null;
        }

        String taskName = String.format("datax_odpswriter_%s_%s", tag, UUID.randomUUID().toString().replace('-', '_'));
        LOG.info("Try to start sqlTask:[{}] to run odps sql:[\n{}\n] .", taskName, query);

        //todo:biz_id set (目前ddl先不做)
        Instance instance;
        Instance.TaskStatus status;
        try {
            instance = SQLTask.run(odps, odps.getDefaultProject(), query, taskName, hints, null);
            instance.waitForSuccess();
            status = instance.getTaskStatus().get(taskName);
            if (!Instance.TaskStatus.Status.SUCCESS.equals(status.getStatus())) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.RUN_SQL_FAILED,
                        MESSAGE_SOURCE.message("odpsutil.15", query));
            }
            return instance;
        } catch (DataXException e) {
            throw e;
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.RUN_SQL_ODPS_EXCEPTION,
                    MESSAGE_SOURCE.message("odpsutil.16", query), e);
        }
    }


    public static String generateTaskName(String tag) {
        return String.format("datax_odpswriter_%s_%s", tag, UUID.randomUUID().toString().replace('-', '_'));
    }
    
    public static void checkBlockComplete(final TableTunnel.UploadSession masterUpload, final Long[] blocks) {
        Long[] serverBlocks;
        try {
            serverBlocks = 
                    RetryUtil.executeWithRetry(new Callable<Long[]>() {
                @Override
                public Long[] call() throws Exception {
                    return masterUpload.getBlockList();
                }
            }, MAX_RETRY_TIME, 1000L, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.COMMIT_BLOCK_FAIL,
                    MESSAGE_SOURCE.message("odpsutil.17", masterUpload.getId()), e);
        }
        
        HashMap<Long, Boolean> serverBlockMap = new HashMap<Long, Boolean>();
        for (Long blockId : serverBlocks) {
          serverBlockMap.put(blockId, true);
        }
        
        for (Long blockId : blocks) {
            if (!serverBlockMap.containsKey(blockId)) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.COMMIT_BLOCK_FAIL,
                        "BlockId[" + blockId + "] upload failed!");
            }
          }
        
    }    
    
    public static void masterComplete(final TableTunnel.UploadSession masterUpload) {
        try {
            RetryUtil.executeWithRetry(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    masterUpload.commit();
                    return null;
                }
            }, MAX_RETRY_TIME, 1000L, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.COMMIT_BLOCK_FAIL,
                    MESSAGE_SOURCE.message("odpsutil.17", masterUpload.getId()), e);
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
                    MESSAGE_SOURCE.message("odpsutil.17", StringUtils.join(blocks, ","), masterUpload.getId()), e);
        }
    }

    public static void slaveWriteOneBlock(final TableTunnel.UploadSession slaveUpload, final ProtobufRecordPack protobufRecordPack,
                                          final long blockId, final Long timeoutInMs) {
        try {
            RetryUtil.executeWithRetry(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    slaveUpload.writeBlock(blockId, protobufRecordPack, timeoutInMs);
                    return null;
                }
            }, MAX_RETRY_TIME, 1000L, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.WRITER_RECORD_FAIL,
                    MESSAGE_SOURCE.message("odpsutil.18", blockId, slaveUpload.getId()), e);
        }

    }

    public static List<Integer> parsePosition(List<String> allColumnList, List<String> allPartColumnList,
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

            if (null != allPartColumnList) {
                for (int i = 0, len = allPartColumnList.size(); i < len; i++) {
                    if (allPartColumnList.get(i).equalsIgnoreCase(col)) {
                        retList.add(-1);
                        hasColumn = true;
                        break;
                    }
                }
            }

            if (!hasColumn) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.COLUMN_NOT_EXIST,
                        MESSAGE_SOURCE.message("odpsutil.19", col));
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
        }
        return allColumns;
    }

    public static List<String> getAllPartColumns(TableSchema schema) {
        if (null == schema) {
            throw new IllegalArgumentException("parameter schema can not be null.");
        }

        List<String> allPartColumns = new ArrayList<>();

        List<Column> partCols = schema.getPartitionColumns();

        for (Column column : partCols) {
            allPartColumns.add(column.getName());
        }

        return allPartColumns;
    }

    public static String getPartColValFromDataXRecord(com.alibaba.datax.common.element.Record dataxRecord,
                                                      List<Integer> positions, List<String> userConfiguredColumns,
                                                      Map<String, DateTransForm> dateTransFormMap) {
        StringBuilder partition = new StringBuilder();
        for (int i = 0, len = dataxRecord.getColumnNumber(); i < len; i++) {
            if (positions.get(i) == -1) {
                if (partition.length() > 0) {
                    partition.append(",");
                }
                String partName = userConfiguredColumns.get(i);
                //todo: 这里应该根据分区列的类型做转换，这里先直接toString转换了
                com.alibaba.datax.common.element.Column partitionCol = dataxRecord.getColumn(i);
                String partVal = partitionCol.getRawData().toString();
                if (StringUtils.isBlank(partVal)) {
                    throw new DataXException(OdpsWriterErrorCode.ILLEGAL_VALUE, String.format(
                            "value of column %s exit null value, it can not be used as partition column", partName));
                }

                // 如果分区列的值的格式是一个日期，并且用户设置列的转换规则
                DateTransForm dateTransForm = null;
                if (null != dateTransFormMap) {
                    dateTransForm = dateTransFormMap.get(partName);
                }
                if (null != dateTransForm) {
                    try {
                        // 日期列
                        if (partitionCol.getType().equals(com.alibaba.datax.common.element.Column.Type.DATE)) {
                            partVal = OdpsUtil.date2StringWithFormat(partitionCol.asDate(), dateTransForm.getToFormat());
                        }
                        // String 列，需要先按照 fromFormat 转换为日期
                        if (partitionCol.getType().equals(com.alibaba.datax.common.element.Column.Type.STRING)) {
                            partVal = OdpsUtil.date2StringWithFormat(partitionCol.asDate(dateTransForm.getFromFormat()), dateTransForm.getToFormat());
                        }
                    } catch (DataXException e) {
                        LOG.warn("Parse {} with format {} error! Please check the column config and {} config. So user original value '{}'. Detail info: {}",
                                partVal, dateTransForm.toString(), Key.PARTITION_COL_MAPPING, partVal, e);
                    }
                }

                partition.append(partName).append("=").append(partVal);
            }
        }
        return partition.toString();
    }

    public static String date2StringWithFormat(Date date, String dateFormat) {
        return  DateFormatUtils.format(date, dateFormat, TimeZone.getTimeZone("GMT+8"));
    }

    public static List<TypeInfo> getTableOriginalColumnTypeList(TableSchema schema) {
        List<TypeInfo> tableOriginalColumnTypeList = new ArrayList<TypeInfo>();

        List<Column> columns = schema.getColumns();
        for (Column column : columns) {
            tableOriginalColumnTypeList.add(column.getTypeInfo());
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
                    throw DataXException.asDataXException(OdpsWriterErrorCode.PARTITION_ERROR, MESSAGE_SOURCE.message("odpsutil.21", table.getName()));
                } else {
                    LOG.info("Try to truncate partition=[{}] in table=[{}].", partition, table.getName());
                    OdpsUtil.truncatePartition(odps, table, partition);
                }
            } else {
                //非分区表
                if (StringUtils.isNotBlank(partition)) {
                    throw DataXException.asDataXException(OdpsWriterErrorCode.PARTITION_ERROR, MESSAGE_SOURCE.message("odpsutil.22", table.getName()));
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
                            MESSAGE_SOURCE.message("odpsutil.23", table.getName()));
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
                            MESSAGE_SOURCE.message("odpsutil.24", table.getName()));
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
                    throw DataXException.asDataXException(OdpsWriterErrorCode.PARTITION_ERROR, MESSAGE_SOURCE.message("odpsutil.25", table.getName()));
                }
            } else {
                //非分区表
                if (StringUtils.isNotBlank(partition)) {
                    throw DataXException.asDataXException(OdpsWriterErrorCode.PARTITION_ERROR, MESSAGE_SOURCE.message("odpsutil.26", table.getName()));
                }
            }
        } else {
            //不需要 truncate
            if (isPartitionedTable) {
                //分区表
                if (StringUtils.isBlank(partition)) {
                    throw DataXException.asDataXException(OdpsWriterErrorCode.PARTITION_ERROR,
                            MESSAGE_SOURCE.message("odpsutil.27", table.getName()));
                }
            } else {
                //非分区表
                if (StringUtils.isNotBlank(partition)) {
                    throw DataXException.asDataXException(OdpsWriterErrorCode.PARTITION_ERROR,
                            MESSAGE_SOURCE.message("odpsutil.28", table.getName()));
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
                        MESSAGE_SOURCE.message("odpsutil.29", tableName), e);
            } else if(e.getMessage().contains(OdpsExceptionMsg.ODPS_TABLE_NOT_FOUNT)) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.ODPS_TABLE_NOT_FOUNT,
                        MESSAGE_SOURCE.message("odpsutil.30", tableName), e);
            } else if(e.getMessage().contains(OdpsExceptionMsg.ODPS_ACCESS_KEY_ID_NOT_FOUND)) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.ODPS_ACCESS_KEY_ID_NOT_FOUND,
                        MESSAGE_SOURCE.message("odpsutil.31", tableName), e);
            } else if(e.getMessage().contains(OdpsExceptionMsg.ODPS_ACCESS_KEY_INVALID)) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.ODPS_ACCESS_KEY_INVALID,
                        MESSAGE_SOURCE.message("odpsutil.32", tableName), e);
            } else if(e.getMessage().contains(OdpsExceptionMsg.ODPS_ACCESS_DENY)) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.ODPS_ACCESS_DENY,
                        MESSAGE_SOURCE.message("odpsutil.33", tableName), e);
            }
        }
        throw DataXException.asDataXException(OdpsWriterErrorCode.ILLEGAL_VALUE,
                MESSAGE_SOURCE.message("odpsutil.34", tableName), e);
    }

    /**
     * count统计数据，自动创建统计表
     * @param tableName 统计表名字
     * @return
     */
    public static String getCreateSummaryTableDDL(String tableName) {
        return String.format("CREATE TABLE IF NOT EXISTS %s " +
                        "(src_table_name STRING, " +
                        "dest_table_name STRING, " +
                        "src_row_num BIGINT, " +
                        "src_query_time DATETIME, " +
                        "read_succeed_records BIGINT," +
                        "write_succeed_records BIGINT," +
                        "dest_row_num BIGINT, " +
                        "write_time DATETIME);",
                tableName);
    }

    /**
     * count统计数据，获取count dml
     * @param tableName
     * @return
     */
    public static String countTableSql(final String tableName, final String partition) {
        if (StringUtils.isNotBlank(partition)) {
            String[] partitions = partition.split("\\,");
            String p = String.join(" and ", partitions);
            return  String.format("SELECT COUNT(1) AS odps_num FROM %s WHERE %s;", tableName, p);
        } else {
            return  String.format("SELECT COUNT(1) AS odps_num FROM %s;", tableName);
        }
    }

    /**
     * count统计数据 dml 对应字段，用于查询
     * @return
     */
    public static String countName() {
        return "odps_num";
    }

    /**
     * count统计数据dml
     * @param summaryTableName 统计数据写入表
     * @param sourceTableName datax reader 表
     * @param destTableName datax writer 表
     * @param srcCount  reader表行数
     * @param queryTime reader表查询时间
     * @param destCount  writer 表行书
     * @return insert dml sql
     */
    public static String getInsertSummaryTableSql(String summaryTableName, String sourceTableName, String destTableName,
                                                  Long srcCount, String queryTime, Number readSucceedRecords,
                                                     Number writeSucceedRecords, Long destCount) {
        final String sql = "INSERT INTO %s (src_table_name,dest_table_name," +
                " src_row_num, src_query_time, read_succeed_records, write_succeed_records, dest_row_num, write_time) VALUES ( %s );";

        String insertData = String.format("'%s', '%s', %s, %s, %s, %s, %s, getdate()",
                sourceTableName, destTableName, srcCount, queryTime, readSucceedRecords, writeSucceedRecords, destCount );
        return String.format(sql, summaryTableName, insertData);
    }

    public static void createTable(Odps odps, String tableName, final String sql) {
        try {
            LOG.info("create table with sql: {}", sql);
            runSqlTaskWithRetry(odps, sql, MAX_RETRY_TIME, 1000, true, "create", null);
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.RUN_SQL_FAILED,
                    MESSAGE_SOURCE.message("odpsutil.7", tableName), e);
        }
    }

    public static void createTableFromTable(Odps odps, String resourceTable, String targetTable) {
        TableSchema schema = odps.tables().get(resourceTable).getSchema();
        StringBuilder builder = new StringBuilder();
        Iterator<Column> iterator = schema.getColumns().iterator();
        while (iterator.hasNext()) {
            Column c = iterator.next();
            builder.append(String.format(" %s %s ", c.getName(), c.getTypeInfo().getTypeName()));
            if (iterator.hasNext()) {
                builder.append(",");
            }
        }
        String createTableSql = String.format("CREATE TABLE IF NOT EXISTS %s (%s);", targetTable, builder.toString());

        try {
            LOG.info("create table with sql: {}", createTableSql);
            runSqlTaskWithRetry(odps, createTableSql, MAX_RETRY_TIME, 1000, true, "create", null);
        } catch (Exception e) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.RUN_SQL_FAILED,
                    MESSAGE_SOURCE.message("odpsutil.7", targetTable), e);
        }
    }

    public static Object truncateSingleFieldData(OdpsType type, Object data, int limit, Boolean enableOverLengthOutput) {
        if (data == null) {
            return data;
        }
        if (OdpsType.STRING.equals(type)) {
            if(enableOverLengthOutput) {
                LOG.warn(
                        "InvalidData: The string's length is more than " + limit + " bytes. content:" + data);
            }
            LOG.info("before truncate string length:" + ((String) data).length());
            //确保特殊字符场景下的截断
            limit -= Constant.UTF8_ENCODED_CHAR_MAX_SIZE;
            data = cutString((String) data, limit);
            LOG.info("after truncate string length:" + ((String) data).length());
        } else if (OdpsType.BINARY.equals(type)) {
            byte[] oriDataBytes = ((Binary) data).data();
            if(oriDataBytes == null){
                return data;
            }
            int originLength = oriDataBytes.length;
            if (originLength <= limit) {
                return data;
            }
            if(enableOverLengthOutput) {
                LOG.warn("InvalidData: The binary's length is more than " + limit + " bytes. content:" + byteArrToHex(oriDataBytes));
            }
            LOG.info("before truncate binary length:" + oriDataBytes.length);
            byte[] newData = new byte[limit];
            System.arraycopy(oriDataBytes, 0, newData, 0, limit);
            LOG.info("after truncate binary length:" + newData.length);
            return new Binary(newData);
        }
        return data;
    }
    public static Object setNull(OdpsType type,Object data, int limit, Boolean enableOverLengthOutput) {
        if (data == null ) {
            return null;
        }
        if (OdpsType.STRING.equals(type)) {
            if(enableOverLengthOutput) {
                LOG.warn(
                        "InvalidData: The string's length is more than " + limit + " bytes. content:" + data);
            }
            return null;
        } else if (OdpsType.BINARY.equals(type)) {
            byte[] oriDataBytes = ((Binary) data).data();
            int originLength = oriDataBytes.length;
            if (originLength > limit) {
                if(enableOverLengthOutput) {
                    LOG.warn("InvalidData: The binary's length is more than " + limit + " bytes. content:" + new String(oriDataBytes));
                }
                return null;
            }
        }
        return data;
    }
    public static boolean validateStringLength(String value, long limit) {
        try {
            if (value.length() * Constant.UTF8_ENCODED_CHAR_MAX_SIZE > limit
                    && value.getBytes("utf-8").length > limit) {
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return true;
        }
        return true;
    }
    public static String cutString(String sourceString, int cutBytes) {
        if (sourceString == null || "".equals(sourceString.trim()) || cutBytes < 1) {
            return "";
        }
        int lastIndex = 0;
        boolean stopFlag = false;
        int totalBytes = 0;
        for (int i = 0; i < sourceString.length(); i++) {
            String s = Integer.toBinaryString(sourceString.charAt(i));
            if (s.length() > 8) {
                totalBytes += 3;
            } else {
                totalBytes += 1;
            }
            if (!stopFlag) {
                if (totalBytes == cutBytes) {
                    lastIndex = i;
                    stopFlag = true;
                } else if (totalBytes > cutBytes) {
                    lastIndex = i - 1;
                    stopFlag = true;
                }
            }
        }
        if (!stopFlag) {
            return sourceString;
        } else {
            return sourceString.substring(0, lastIndex + 1);
        }
    }
    public static boolean dataOverLength(OdpsType type, Object data, int limit){
        if (data == null ) {
            return false;
        }
        if (OdpsType.STRING.equals(type)) {
            if(!OdpsUtil.validateStringLength((String)data, limit)){
                return true;
            }
        }else if (OdpsType.BINARY.equals(type)){
            byte[] oriDataBytes = ((Binary) data).data();
            if(oriDataBytes == null){
                return false;
            }
            int originLength = oriDataBytes.length;
            if (originLength > limit) {
                return true;
            }
        }
        return false;
    }
    public static Object processOverLengthData(Object data, OdpsType type, String overLengthRule, int maxFieldLength, Boolean enableOverLengthOutput) {
        try{
            //超长数据检查
            if(OdpsWriter.maxOutputOverLengthRecord != null && OdpsWriter.globalTotalTruncatedRecordNumber.get() >= OdpsWriter.maxOutputOverLengthRecord){
                enableOverLengthOutput = false;
            }
            if ("truncate".equalsIgnoreCase(overLengthRule)) {
                if (OdpsUtil.dataOverLength(type, data, OdpsWriter.maxOdpsFieldLength)) {
                    Object newData = OdpsUtil.truncateSingleFieldData(type, data, maxFieldLength, enableOverLengthOutput);
                    OdpsWriter.globalTotalTruncatedRecordNumber.incrementAndGet();
                    return newData;
                }
            } else if ("setNull".equalsIgnoreCase(overLengthRule)) {
                if (OdpsUtil.dataOverLength(type, data, OdpsWriter.maxOdpsFieldLength)) {
                    OdpsWriter.globalTotalTruncatedRecordNumber.incrementAndGet();
                    return OdpsUtil.setNull(type, data, maxFieldLength, enableOverLengthOutput);
                }
            }
        }catch (Throwable e){
            LOG.warn("truncate overLength data failed!",  e);
        }
        return data;
    }
    private static final char HEX_CHAR_ARR[] = {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
    /**
     * 字节数组转十六进制字符串
     * @param btArr
     * @return
     */
    public static String byteArrToHex(byte[] btArr) {
        char strArr[] = new char[btArr.length * 2];
        int i = 0;
        for (byte bt : btArr) {
            strArr[i++] = HEX_CHAR_ARR[bt>>>4 & 0xf];
            strArr[i++] = HEX_CHAR_ARR[bt & 0xf];
        }
        return new String(strArr);
    }
    public static byte[] hexToByteArr(String hexStr) {
        char[] charArr = hexStr.toCharArray();
        byte btArr[] = new byte[charArr.length / 2];
        int index = 0;
        for (int i = 0; i < charArr.length; i++) {
            int highBit = hexStr.indexOf(charArr[i]);
            int lowBit = hexStr.indexOf(charArr[++i]);
            btArr[index] = (byte) (highBit << 4 | lowBit);
            index++;
        }
        return btArr;
    }

}
