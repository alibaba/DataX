package com.alibaba.datax.plugin.reader.odpsreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.FilterUtil;
import com.alibaba.datax.plugin.reader.odpsreader.util.IdAndKeyUtil;
import com.alibaba.datax.plugin.reader.odpsreader.util.OdpsSplitUtil;
import com.alibaba.datax.plugin.reader.odpsreader.util.OdpsUtil;
import com.aliyun.odps.*;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OdpsReader extends Reader {
    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private static boolean IS_DEBUG = LOG.isDebugEnabled();

        private Configuration originalConfig;
        private Odps odps;
        private Table table;

        public void preCheck() {
            this.init();
        }


        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            //如果用户没有配置accessId/accessKey,尝试从环境变量获取
            String accountType = originalConfig.getString(Key.ACCOUNT_TYPE, Constant.DEFAULT_ACCOUNT_TYPE);
            if (Constant.DEFAULT_ACCOUNT_TYPE.equalsIgnoreCase(accountType)) {
                this.originalConfig = IdAndKeyUtil.parseAccessIdAndKey(this.originalConfig);
            }

            //检查必要的参数配置
            OdpsUtil.checkNecessaryConfig(this.originalConfig);
            //重试次数的配置检查
            OdpsUtil.dealMaxRetryTime(this.originalConfig);

            //确定切分模式
            dealSplitMode(this.originalConfig);

            this.odps = OdpsUtil.initOdps(this.originalConfig);
            String tableName = this.originalConfig.getString(Key.TABLE);
            String projectName = this.originalConfig.getString(Key.PROJECT);

            this.table = OdpsUtil.getTable(this.odps, projectName, tableName);
            this.originalConfig.set(Constant.IS_PARTITIONED_TABLE,
                    OdpsUtil.isPartitionedTable(table));

            boolean isVirtualView = this.table.isVirtualView();
            if (isVirtualView) {
                throw DataXException.asDataXException(OdpsReaderErrorCode.VIRTUAL_VIEW_NOT_SUPPORT,
                        String.format("源头表:%s 是虚拟视图，DataX 不支持读取虚拟视图.", tableName));
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
                        String.format("您所配置的 splitMode:%s 不正确. splitMode 仅允许配置为 record 或者 partition.", splitMode));
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
                            String.format("分区信息没有配置.由于源头表:%s 为分区表, 所以您需要配置其抽取的表的分区信息. 格式形如:pt=hello,ds=hangzhou，请您参考此格式修改该配置项.",
                                    table.getName()));
                } else {
                    List<String> allPartitions = OdpsUtil.getTableAllPartitions(table);

                    if (null == allPartitions || allPartitions.isEmpty()) {
                        throw DataXException.asDataXException(OdpsReaderErrorCode.PARTITION_ERROR,
                                String.format("分区信息配置错误.源头表:%s 虽然为分区表, 但其实际分区值并不存在. 请确认源头表已经生成该分区，再进行数据抽取.",
                                        table.getName()));
                    }

                    List<String> parsedPartitions = expandUserConfiguredPartition(
                            allPartitions, userConfiguredPartitions);

                    if (null == parsedPartitions || parsedPartitions.isEmpty()) {
                        throw DataXException.asDataXException(
                                OdpsReaderErrorCode.PARTITION_ERROR,
                                String.format(
                                        "分区配置错误，根据您所配置的分区没有匹配到源头表中的分区. 源头表所有分区是:[\n%s\n], 您配置的分区是:[\n%s\n]. 请您根据实际情况在作出修改. ",
                                        StringUtils.join(allPartitions, "\n"),
                                        StringUtils.join(userConfiguredPartitions, "\n")));
                    }
                    this.originalConfig.set(Key.PARTITION, parsedPartitions);
                    
                    for (Column column : table.getSchema()
                            .getPartitionColumns()) {
                        partitionColumns.add(column.getName());
                    }
                }
            } else {
                // 非分区表，则不能配置分区
                if (null != userConfiguredPartitions
                        && !userConfiguredPartitions.isEmpty()) {
                    throw DataXException.asDataXException(OdpsReaderErrorCode.PARTITION_ERROR,
                            String.format("分区配置错误，源头表:%s 为非分区表, 您不能配置分区. 请您删除该配置项. ", table.getName()));
                }
            }
            
            this.originalConfig.set(Constant.PARTITION_COLUMNS, partitionColumns);
            if (isPartitionedTable) {
                LOG.info("{源头表:{} 的所有分区列是:[{}]}", table.getName(),
                        StringUtils.join(partitionColumns, ","));
            }
        }

        private List<String> expandUserConfiguredPartition(
                List<String> allPartitions, List<String> userConfiguredPartitions) {
            // 对odps 本身的所有分区进行特殊字符的处理
            List<String> allStandardPartitions = OdpsUtil
                    .formatPartitions(allPartitions);

            // 对用户自身配置的所有分区进行特殊字符的处理
            List<String> allStandardUserConfiguredPartitions = OdpsUtil
                    .formatPartitions(userConfiguredPartitions);

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
                            String.format("分区配置错误, 您所配置的分区级数和该表的实际情况不一致, 比如分区:[%s] 是 %s 级分区, 而分区:[%s] 是 %s 级分区. DataX 是通过英文逗号判断您所配置的分区级数的. 正确的格式形如\"pt=${bizdate}, type=0\" ，请您参考示例修改该配置项. ",
                                    firstPartition, firstPartitionDepth, comparedPartition, comparedPartitionDepth));
                }
            }

            int tableOriginalPartitionDepth = allStandardPartitions.get(0).split(",").length;
            if (firstPartitionDepth != tableOriginalPartitionDepth) {
                throw DataXException.asDataXException(OdpsReaderErrorCode.PARTITION_ERROR,
                        String.format("分区配置错误, 您所配置的分区:%s 的级数:%s 与您要读取的 ODPS 源头表的分区级数:%s 不相等. DataX 是通过英文逗号判断您所配置的分区级数的.正确的格式形如\"pt=${bizdate}, type=0\" ，请您参考示例修改该配置项.",
                                firstPartition, firstPartitionDepth, tableOriginalPartitionDepth));
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

            LOG.info("源头表:{} 的所有字段是:[{}]", table.getName(), columnMeta.toString());

            if (1 == userConfiguredColumns.size()
                    && "*".equals(userConfiguredColumns.get(0))) {
                LOG.warn("这是一条警告信息，您配置的 ODPS 读取的列为*，这是不推荐的行为，因为当您的表字段个数、类型有变动时，可能影响任务正确性甚至会运行出错. 建议您把所有需要抽取的列都配置上. ");
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
            List<Pair<String, ColumnType>> parsedColumns = OdpsUtil
                    .parseColumns(allNormalColumns, allPartitionColumns,
                            userConfiguredColumns);

            this.originalConfig.set(Constant.PARSED_COLUMNS, parsedColumns);

            StringBuilder sb = new StringBuilder();
            sb.append("[ ");
            for (int i = 0, len = parsedColumns.size(); i < len; i++) {
                Pair<String, ColumnType> pair = parsedColumns.get(i);
                sb.append(String.format(" %s : %s", pair.getLeft(),
                        pair.getRight()));
                if (i != len - 1) {
                    sb.append(",");
                }
            }
            sb.append(" ]");
            LOG.info("parsed column details: {} .", sb.toString());
        }


        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return OdpsSplitUtil.doSplit(this.originalConfig, this.odps, adviceNumber);
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private Configuration readerSliceConf;

        private String tunnelServer;
        private Odps odps = null;
        private Table table = null;
        private String projectName = null;
        private String tableName = null;
        private boolean isPartitionedTable;
        private String sessionId;
        private boolean isCompress;

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

            // sessionId 为空的情况是：切分级别只到 partition 的情况
            if (StringUtils.isBlank(this.sessionId)) {
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
                LOG.warn(String.format("源头表:%s 的分区:%s 没有内容可抽取, 请您知晓.",
                        this.tableName, partition));
                return;
            } else {
                throw DataXException.asDataXException(OdpsReaderErrorCode.READ_DATA_FAIL,
                        String.format("源头表:%s 的分区:%s  读取行数为负数, 请联系 ODPS 管理员查看表状态!",
                                this.tableName, partition));
            }
            
            TableSchema tableSchema = this.table.getSchema();
            Set<Column> allColumns = new HashSet<Column>();
            allColumns.addAll(tableSchema.getColumns());
            allColumns.addAll(tableSchema.getPartitionColumns());

            Map<String, OdpsType> columnTypeMap = new HashMap<String, OdpsType>();
            for (Column column : allColumns) {
                columnTypeMap.put(column.getName(), column.getType());
            }

            try {
                List<Configuration> parsedColumnsTmp = this.readerSliceConf
                        .getListConfiguration(Constant.PARSED_COLUMNS);
                List<Pair<String, ColumnType>> parsedColumns = new ArrayList<Pair<String, ColumnType>>();
                for (int i = 0; i < parsedColumnsTmp.size(); i++) {
                    Configuration eachColumnConfig = parsedColumnsTmp.get(i);
                    String columnName = eachColumnConfig.getString("left");
                    ColumnType columnType = ColumnType
                            .asColumnType(eachColumnConfig.getString("right"));
                    parsedColumns.add(new MutablePair<String, ColumnType>(
                            columnName, columnType));

                }
                ReaderProxy readerProxy = new ReaderProxy(recordSender, downloadSession,
                        columnTypeMap, parsedColumns, partition, this.isPartitionedTable,
                        start, count, this.isCompress);

                readerProxy.doRead();
            } catch (Exception e) {
                throw DataXException.asDataXException(OdpsReaderErrorCode.READ_DATA_FAIL,
                        String.format("源头表:%s 的分区:%s 读取失败, 请联系 ODPS 管理员查看错误详情.", this.tableName, partition), e);
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
