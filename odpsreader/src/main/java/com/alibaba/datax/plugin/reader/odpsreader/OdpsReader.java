package com.alibaba.datax.plugin.reader.odpsreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.FilterUtil;
import com.alibaba.datax.common.util.MessageSource;
import com.alibaba.datax.plugin.reader.odpsreader.util.*;
import com.alibaba.fastjson2.JSON;
import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.type.TypeInfo;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class OdpsReader extends Reader {
    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory
            .getLogger(Job.class);
        private static final MessageSource MESSAGE_SOURCE = MessageSource.loadResourceBundle(OdpsReaderErrorCode.class, Locale.ENGLISH, MessageSource.timeZone);

        private Configuration originalConfig;
        private boolean successOnNoPartition;
        private Odps odps;
        private Table table;

        @Override
        public void preCheck() {
            this.init();
            this.prepare();
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.successOnNoPartition = this.originalConfig.getBool(Key.SUCCESS_ON_NO_PATITION, false);

            //检查必要的参数配置
            OdpsUtil.checkNecessaryConfig(this.originalConfig);
            //重试次数的配置检查
            OdpsUtil.dealMaxRetryTime(this.originalConfig);

            //确定切分模式
            dealSplitMode(this.originalConfig);

            this.odps = OdpsUtil.initOdps(this.originalConfig);

        }

        private void initOdpsTableInfo() {
            String tableName = this.originalConfig.getString(Key.TABLE);
            String projectName = this.originalConfig.getString(Key.PROJECT);

            this.table = OdpsUtil.getTable(this.odps, projectName, tableName);
            this.originalConfig.set(Constant.IS_PARTITIONED_TABLE,
                OdpsUtil.isPartitionedTable(table));

            boolean isVirtualView = this.table.isVirtualView();
            if (isVirtualView) {
                throw DataXException.asDataXException(OdpsReaderErrorCode.VIRTUAL_VIEW_NOT_SUPPORT,
                    MESSAGE_SOURCE.message("odpsreader.1", tableName));
            }

            this.dealPartition(this.table);
            this.dealColumn(this.table);
        }

        private void dealSplitMode(Configuration originalConfig) {
            String splitMode = originalConfig.getString(Key.SPLIT_MODE, Constant.DEFAULT_SPLIT_MODE).trim();
            if (splitMode.equalsIgnoreCase(Constant.DEFAULT_SPLIT_MODE) ||
                splitMode.equalsIgnoreCase(Constant.PARTITION_SPLIT_MODE)) {
                originalConfig.set(Key.SPLIT_MODE, splitMode);
            } else {
                throw DataXException.asDataXException(OdpsReaderErrorCode.SPLIT_MODE_ERROR,
                    MESSAGE_SOURCE.message("odpsreader.2", splitMode));
            }
        }

        /**
         * 对分区的配置处理。最终效果是所有正则配置，完全展开成实际对应的分区配置。正则规则如下：
         * <p/>
         * <ol>
         * <li>如果是分区表，则必须配置分区：可以配置为*,表示整表读取；也可以配置为分别列出要读取的叶子分区. </br>TODO
         * 未来会支持一些常用的分区正则筛选配置. 分区配置中，不能在分区所表示的数组中配置多个*，因为那样就是多次读取全表，无意义.</li>
         * <li>如果是非分区表，则不能配置分区值.</li>
         * </ol>
         */
        private void dealPartition(Table table) {
            List<String> userConfiguredPartitions = this.originalConfig.getList(
                Key.PARTITION, String.class);

            boolean isPartitionedTable = this.originalConfig.getBool(Constant.IS_PARTITIONED_TABLE);
            List<String> partitionColumns = new ArrayList<String>();

            if (isPartitionedTable) {
                // 分区表，需要配置分区
                if (null == userConfiguredPartitions || userConfiguredPartitions.isEmpty()) {
                    throw DataXException.asDataXException(OdpsReaderErrorCode.PARTITION_ERROR,
                        MESSAGE_SOURCE.message("odpsreader.3", table.getName()));
                } else {
                    // 获取分区列名, 支持用户配置分区列同步
                    for (Column column : table.getSchema().getPartitionColumns()) {
                        partitionColumns.add(column.getName());
                    }

                    List<String> allPartitions = OdpsUtil.getTableAllPartitions(table);

                    List<String> parsedPartitions = expandUserConfiguredPartition(
                        table, allPartitions, userConfiguredPartitions, partitionColumns.size());
                    if (null == parsedPartitions || parsedPartitions.isEmpty()) {
                        if (!this.successOnNoPartition) {
                            // PARTITION_NOT_EXISTS_ERROR 这个异常ErrorCode在AdsWriter有使用，用户判断空分区Load Data任务不报错
                            // 其他类型的异常不要使用这个错误码
                            throw DataXException.asDataXException(
                                OdpsReaderErrorCode.PARTITION_NOT_EXISTS_ERROR,
                                MESSAGE_SOURCE.message("odpsreader.5",
                                    StringUtils.join(allPartitions, "\n"),
                                    StringUtils.join(userConfiguredPartitions, "\n")));
                        } else {
                            LOG.warn(
                                String.format(
                                    "The partition configuration is wrong, " +
                                            "but you have configured the successOnNoPartition to be true to ignore the error. " +
                                            "According to the partition you have configured, it does not match the partition in the source table. " +
                                            "All the partitions in the source table are:[\n%s\n], the partition you configured is:[\n%s\n]. " +
                                            "please revise it according to the actual situation.",
                                    StringUtils.join(allPartitions, "\n"),
                                    StringUtils.join(userConfiguredPartitions, "\n")));
                        }
                    }
                    LOG.info(String
                        .format("expand user configured partitions are : %s", JSON.toJSONString(parsedPartitions)));
                    this.originalConfig.set(Key.PARTITION, parsedPartitions);
                }
            } else {
                // 非分区表，则不能配置分区
                if (null != userConfiguredPartitions
                    && !userConfiguredPartitions.isEmpty()) {
                    throw DataXException.asDataXException(OdpsReaderErrorCode.PARTITION_ERROR,
                        MESSAGE_SOURCE.message("odpsreader.6", table.getName()));
                }
            }

            this.originalConfig.set(Constant.PARTITION_COLUMNS, partitionColumns);
            if (isPartitionedTable) {
                LOG.info(MESSAGE_SOURCE.message("odpsreader.7", table.getName(),
                    StringUtils.join(partitionColumns, ",")));
            }
        }

        /**
         * 将用户配置的分区(可能是直接的分区配置 dt=20170101, 可能是简单正则dt=201701*, 也可能是区间过滤条件 dt>=20170101 and dt<20170130) 和ODPS
         * table所有的分区进行匹配,过滤出用户希望同步的分区集合
         *
         * @param table                       odps table
         * @param allPartitions               odps table所有的分区
         * @param userConfiguredPartitions    用户配置的分区
         * @param tableOriginalPartitionDepth odps table分区级数(一级分区,二级分区,三级分区等)
         * @return 返回过滤出的分区
         */
        private List<String> expandUserConfiguredPartition(Table table,
                                                           List<String> allPartitions,
                                                           List<String> userConfiguredPartitions,
                                                           int tableOriginalPartitionDepth) {

            UserConfiguredPartitionClassification userConfiguredPartitionClassification = OdpsUtil
                .classifyUserConfiguredPartitions(userConfiguredPartitions);

            if (userConfiguredPartitionClassification.isIncludeHintPartition()) {
                List<String> expandUserConfiguredPartitionResult = new ArrayList<String>();

                // 处理不包含/*query*/的分区过滤
                if (!userConfiguredPartitionClassification.getUserConfiguredNormalPartition().isEmpty()) {
                    expandUserConfiguredPartitionResult.addAll(expandNoHintUserConfiguredPartition(allPartitions,
                        userConfiguredPartitionClassification.getUserConfiguredNormalPartition(),
                        tableOriginalPartitionDepth));
                }
                if (!allPartitions.isEmpty()) {
                    expandUserConfiguredPartitionResult.addAll(expandHintUserConfiguredPartition(table,
                        allPartitions, userConfiguredPartitionClassification.getUserConfiguredHintPartition()));
                }
                return expandUserConfiguredPartitionResult;
            } else {
                return expandNoHintUserConfiguredPartition(allPartitions, userConfiguredPartitions,
                    tableOriginalPartitionDepth);
            }
        }

        /**
         * 匹配包含 HINT 条件的过滤
         *
         * @param table                        odps table
         * @param allPartitions                odps table所有的分区
         * @param userHintConfiguredPartitions 用户配置的分区
         * @return 返回过滤出的分区
         */
        private List<String> expandHintUserConfiguredPartition(Table table,
                                                               List<String> allPartitions,
                                                               List<String> userHintConfiguredPartitions) {
            try {
                // load odps table all partitions into sqlite memory database
                SqliteUtil sqliteUtil = new SqliteUtil();
                sqliteUtil.loadAllPartitionsIntoSqlite(table, allPartitions);
                return sqliteUtil.selectUserConfiguredPartition(userHintConfiguredPartitions);
            } catch (Exception ex) {
                throw DataXException.asDataXException(OdpsReaderErrorCode.PARTITION_ERROR,
                    String.format("Expand user configured partition has exception: %s", ex.getMessage()), ex);
            }
        }

        /**
         * 匹配没有 HINT 条件的过滤,包括 简单正则匹配(dt=201701*) 和 直接匹配(dt=20170101)
         *
         * @param allPartitions                  odps table所有的分区
         * @param userNormalConfiguredPartitions 用户配置的分区
         * @param tableOriginalPartitionDepth    odps table分区级数(一级分区,二级分区,三级分区等)
         * @return 返回过滤出的分区
         */
        private List<String> expandNoHintUserConfiguredPartition(List<String> allPartitions,
                                                                 List<String> userNormalConfiguredPartitions,
                                                                 int tableOriginalPartitionDepth) {
            // 对odps 本身的所有分区进行特殊字符的处理
            LOG.info("format partition with rules:  remove all space;  remove all ';  replace / to ,");
            // 表里面已有分区量比较大，有些任务无关，没有打印
            List<String> allStandardPartitions = OdpsUtil
                .formatPartitions(allPartitions);

            // 对用户自身配置的所有分区进行特殊字符的处理
            List<String> allStandardUserConfiguredPartitions = OdpsUtil
                .formatPartitions(userNormalConfiguredPartitions);
            LOG.info("user configured partition: {}", JSON.toJSONString(userNormalConfiguredPartitions));
            LOG.info("formated partition: {}", JSON.toJSONString(allStandardUserConfiguredPartitions));

            /**
             *  对配置的分区级数(深度)进行检查
             *  (1)先检查用户配置的分区级数,自身级数是否相等
             *  (2)检查用户配置的分区级数是否与源头表的的分区级数一样
             */
            String firstPartition = allStandardUserConfiguredPartitions.get(0);
            int firstPartitionDepth = firstPartition.split(",").length;

            String comparedPartition = null;
            int comparedPartitionDepth = -1;
            for (int i = 1, len = allStandardUserConfiguredPartitions.size(); i < len; i++) {
                comparedPartition = allStandardUserConfiguredPartitions.get(i);
                comparedPartitionDepth = comparedPartition.split(",").length;
                if (comparedPartitionDepth != firstPartitionDepth) {
                    throw DataXException.asDataXException(OdpsReaderErrorCode.PARTITION_ERROR,
                        MESSAGE_SOURCE
                            .message("odpsreader.8", firstPartition, firstPartitionDepth, comparedPartition,
                                comparedPartitionDepth));
                }
            }

            if (firstPartitionDepth != tableOriginalPartitionDepth) {
                throw DataXException.asDataXException(OdpsReaderErrorCode.PARTITION_ERROR,
                    MESSAGE_SOURCE
                        .message("odpsreader.9", firstPartition, firstPartitionDepth, tableOriginalPartitionDepth));
            }

            List<String> retPartitions = FilterUtil.filterByRegulars(allStandardPartitions,
                allStandardUserConfiguredPartitions);

            return retPartitions;
        }

        private void dealColumn(Table table) {
            // 用户配置的 column 之前已经确保其不为空
            List<String> userConfiguredColumns = this.originalConfig.getList(
                Key.COLUMN, String.class);

            List<Column> allColumns = OdpsUtil.getTableAllColumns(table);
            List<String> allNormalColumns = OdpsUtil
                .getTableOriginalColumnNameList(allColumns);

            StringBuilder columnMeta = new StringBuilder();
            for (Column column : allColumns) {
                columnMeta.append(column.getName()).append(":").append(column.getType()).append(",");
            }
            columnMeta.setLength(columnMeta.length() - 1);

            LOG.info(MESSAGE_SOURCE.message("odpsreader.10", table.getName(), columnMeta.toString()));

            if (1 == userConfiguredColumns.size()
                && "*".equals(userConfiguredColumns.get(0))) {
                LOG.warn(MESSAGE_SOURCE.message("odpsreader.11"));
                this.originalConfig.set(Key.COLUMN, allNormalColumns);
            }

            userConfiguredColumns = this.originalConfig.getList(
                Key.COLUMN, String.class);

            /**
             * warn: 字符串常量需要与表原生字段tableOriginalColumnNameList 分开存放 demo:
             * ["id","'id'","name"]
             */
            List<String> allPartitionColumns = this.originalConfig.getList(
                Constant.PARTITION_COLUMNS, String.class);
            List<InternalColumnInfo> parsedColumns = OdpsUtil
                .parseColumns(allNormalColumns, allPartitionColumns,
                    userConfiguredColumns);

            this.originalConfig.set(Constant.PARSED_COLUMNS, parsedColumns);

            StringBuilder sb = new StringBuilder();
            sb.append("[ ");
            for (int i = 0, len = parsedColumns.size(); i < len; i++) {
            	InternalColumnInfo pair = parsedColumns.get(i);
                sb.append(String.format(" %s : %s", pair.getColumnName(),
                    pair.getColumnType()));
                if (i != len - 1) {
                    sb.append(",");
                }
            }
            
            
            sb.append(" ]");
            LOG.info("parsed column details: {} .", sb.toString());
        }

        @Override
        public void prepare() {
            List<String> preSqls = this.originalConfig.getList(Key.PRE_SQL, String.class);
            if (preSqls != null && !preSqls.isEmpty()) {
                LOG.info(
                    String.format("Beigin to exectue preSql : %s. \n Attention: these preSqls must be idempotent!!!",
                        JSON.toJSONString(preSqls)));
                long beginTime = System.currentTimeMillis();

                StringBuffer preSqlBuffer = new StringBuffer();
                for (String preSql : preSqls) {
                    preSql = preSql.trim();
                    if (StringUtils.isNotBlank(preSql) && !preSql.endsWith(";")) {
                        preSql = String.format("%s;", preSql);
                    }
                    if (StringUtils.isNotBlank(preSql)) {
                        preSqlBuffer.append(preSql);
                    }
                }
                if (StringUtils.isNotBlank(preSqlBuffer.toString())) {
                    OdpsUtil.runSqlTaskWithRetry(this.odps, preSqlBuffer.toString(), "preSql");
                } else {
                    LOG.info("skip to execute the preSql: {}", JSON.toJSONString(preSqls));
                }
                long endTime = System.currentTimeMillis();

                LOG.info(
                    String.format("Exectue odpsreader preSql successfully! cost time: %s ms.", (endTime - beginTime)));
            }
            this.initOdpsTableInfo();
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return OdpsSplitUtil.doSplit(this.originalConfig, this.odps, adviceNumber);
        }

        @Override
        public void post() {
            List<String> postSqls = this.originalConfig.getList(Key.POST_SQL, String.class);

            if (postSqls != null && !postSqls.isEmpty()) {
                LOG.info(
                    String.format("Beigin to exectue postSql : %s. \n Attention: these postSqls must be idempotent!!!",
                        JSON.toJSONString(postSqls)));
                long beginTime = System.currentTimeMillis();
                StringBuffer postSqlBuffer = new StringBuffer();
                for (String postSql : postSqls) {
                    postSql = postSql.trim();
                    if (StringUtils.isNotBlank(postSql) && !postSql.endsWith(";")) {
                        postSql = String.format("%s;", postSql);
                    }
                    if (StringUtils.isNotBlank(postSql)) {
                        postSqlBuffer.append(postSql);
                    }
                }
                if (StringUtils.isNotBlank(postSqlBuffer.toString())) {
                    OdpsUtil.runSqlTaskWithRetry(this.odps, postSqlBuffer.toString(), "postSql");
                } else {
                    LOG.info("skip to execute the postSql: {}", JSON.toJSONString(postSqls));
                }

                long endTime = System.currentTimeMillis();
                LOG.info(
                    String.format("Exectue odpsreader postSql successfully! cost time: %s ms.", (endTime - beginTime)));
            }
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private static final MessageSource MESSAGE_SOURCE = MessageSource.loadResourceBundle(OdpsReader.class);
        private Configuration readerSliceConf;

        private String tunnelServer;
        private Odps odps = null;
        private Table table = null;
        private String projectName = null;
        private String tableName = null;
        private boolean isPartitionedTable;
        private String sessionId;
        private boolean isCompress;
        private boolean successOnNoPartition;

        @Override
        public void init() {
            this.readerSliceConf = super.getPluginJobConf();
            this.tunnelServer = this.readerSliceConf.getString(
                Key.TUNNEL_SERVER, null);

            this.odps = OdpsUtil.initOdps(this.readerSliceConf);
            this.projectName = this.readerSliceConf.getString(Key.PROJECT);
            this.tableName = this.readerSliceConf.getString(Key.TABLE);
            this.table = OdpsUtil.getTable(this.odps, projectName, tableName);
            this.isPartitionedTable = this.readerSliceConf
                .getBool(Constant.IS_PARTITIONED_TABLE);
            this.sessionId = this.readerSliceConf.getString(Constant.SESSION_ID, null);
            this.isCompress = this.readerSliceConf.getBool(Key.IS_COMPRESS, false);
            this.successOnNoPartition = this.readerSliceConf.getBool(Key.SUCCESS_ON_NO_PATITION, false);

            // sessionId 为空的情况是：切分级别只到 partition 的情况
            String partition = this.readerSliceConf.getString(Key.PARTITION);

            // 没有分区读取时, 是没有sessionId这些的
            if (this.isPartitionedTable && StringUtils.isBlank(partition) && this.successOnNoPartition) {
                LOG.warn("Partition is blank, but you config successOnNoPartition[true] ,don't need to create session");
            } else if (StringUtils.isBlank(this.sessionId)) {
                DownloadSession session = OdpsUtil.createMasterSessionForPartitionedTable(odps,
                    tunnelServer, projectName, tableName, this.readerSliceConf.getString(Key.PARTITION));
                this.sessionId = session.getId();
            }
            LOG.info("sessionId:{}", this.sessionId);
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startRead(RecordSender recordSender) {
            DownloadSession downloadSession = null;
            String partition = this.readerSliceConf.getString(Key.PARTITION);

            if (this.isPartitionedTable && StringUtils.isBlank(partition) && this.successOnNoPartition) {
                LOG.warn(String.format(
                    "Partition is blank,not need to be read"));
                recordSender.flush();
                return;
            }

            if (this.isPartitionedTable) {
                downloadSession = OdpsUtil.getSlaveSessionForPartitionedTable(this.odps, this.sessionId,
                    this.tunnelServer, this.projectName, this.tableName, partition);
            } else {
                downloadSession = OdpsUtil.getSlaveSessionForNonPartitionedTable(this.odps, this.sessionId,
                    this.tunnelServer, this.projectName, this.tableName);
            }

            long start = this.readerSliceConf.getLong(Constant.START_INDEX, 0);
            long count = this.readerSliceConf.getLong(Constant.STEP_COUNT,
                downloadSession.getRecordCount());

            if (count > 0) {
                LOG.info(String.format(
                    "Begin to read ODPS table:%s, partition:%s, startIndex:%s, count:%s.",
                    this.tableName, partition, start, count));
            } else if (count == 0) {
                LOG.warn(MESSAGE_SOURCE.message("odpsreader.12", this.tableName, partition));
                return;
            } else {
                throw DataXException.asDataXException(OdpsReaderErrorCode.READ_DATA_FAIL,
                    MESSAGE_SOURCE.message("odpsreader.13", this.tableName, partition));
            }

            TableSchema tableSchema = this.table.getSchema();
            Set<Column> allColumns = new HashSet<Column>();
            allColumns.addAll(tableSchema.getColumns());
            allColumns.addAll(tableSchema.getPartitionColumns());

            Map<String, TypeInfo> columnTypeMap = new HashMap<String, TypeInfo>();
            for (Column column : allColumns) {
                columnTypeMap.put(column.getName(), column.getTypeInfo());
            }

            try {
				List<InternalColumnInfo> parsedColumns = this.readerSliceConf.getListWithJson(Constant.PARSED_COLUMNS,
						InternalColumnInfo.class);
                ReaderProxy readerProxy = new ReaderProxy(recordSender, downloadSession,
                        columnTypeMap, parsedColumns, partition, this.isPartitionedTable,
                        start, count, this.isCompress, this.readerSliceConf);
                readerProxy.doRead();
            } catch (Exception e) {
                throw DataXException.asDataXException(OdpsReaderErrorCode.READ_DATA_FAIL,
                    MESSAGE_SOURCE.message("odpsreader.14", this.tableName, partition), e);
            }

        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

    }
}
