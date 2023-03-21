package com.alibaba.datax.plugin.writer.odpswriter;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.ListUtil;
import com.alibaba.datax.common.util.MessageSource;
import com.alibaba.datax.plugin.writer.odpswriter.model.PartitionInfo;
import com.alibaba.datax.plugin.writer.odpswriter.model.UserDefinedFunction;
import com.alibaba.datax.plugin.writer.odpswriter.util.*;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.aliyun.odps.Odps;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.tunnel.TableTunnel;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.alibaba.datax.plugin.writer.odpswriter.util.CustomPartitionUtils.getListWithJson;

/**
 * 已修改为：每个 task 各自创建自己的 upload,拥有自己的 uploadId，并在 task 中完成对对应 block 的提交。
 */
public class OdpsWriter extends Writer {

    public static HashSet<String> partitionsDealedTruncate = new HashSet<>();
    static final Object lockForPartitionDealedTruncate = new Object();
    public static AtomicInteger partitionCnt = new AtomicInteger(0);
    public static Long maxPartitionCnt;
    public static AtomicLong globalTotalTruncatedRecordNumber = new AtomicLong(0);
    public static Long maxOutputOverLengthRecord;
    public static int maxOdpsFieldLength = Constant.DEFAULT_FIELD_MAX_SIZE;

    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);
        private static final MessageSource MESSAGE_SOURCE = MessageSource.loadResourceBundle(OdpsWriter.class);

        private static final boolean IS_DEBUG = LOG.isDebugEnabled();

        private Configuration originalConfig;
        private Odps odps;
        private Table table;

        private String projectName;
        private String tableName;
        private String tunnelServer;
        private String partition;
        private boolean truncate;
        private String uploadId;
        private TableTunnel.UploadSession masterUpload;
        private int blockSizeInMB;
        private boolean consistencyCommit;
        private boolean supportDynamicPartition;

        public void preCheck() {
            this.init();
            this.doPreCheck();
        }

        public void doPreCheck() {

            //检查列信息是否正确
            List<String> allColumns = OdpsUtil.getAllColumns(this.table.getSchema());
            LOG.info("allColumnList: {} .", StringUtils.join(allColumns, ','));
            List<String> allPartColumns = OdpsUtil.getAllPartColumns(this.table.getSchema());
            LOG.info("allPartColumnsList: {} .", StringUtils.join(allPartColumns, ','));
            dealColumn(this.originalConfig, allColumns, allPartColumns);

            //检查分区信息是否正确
            if (!supportDynamicPartition) {
                OdpsUtil.preCheckPartition(this.odps, this.table, this.partition, this.truncate);
            }
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();


            OdpsUtil.checkNecessaryConfig(this.originalConfig);
            OdpsUtil.dealMaxRetryTime(this.originalConfig);



            this.projectName = this.originalConfig.getString(Key.PROJECT);
            this.tableName = this.originalConfig.getString(Key.TABLE);
            this.tunnelServer = this.originalConfig.getString(Key.TUNNEL_SERVER, null);

            // init odps config
            this.odps = OdpsUtil.initOdpsProject(this.originalConfig);

            //检查表等配置是否正确
            this.table = OdpsUtil.getTable(odps, this.projectName, this.tableName);

            // 处理动态分区参数，以及动态分区相关配置是否合法，如果没有配置动态分区，则根据列映射配置决定是否启用
            this.dealDynamicPartition();

            //check isCompress
            this.originalConfig.getBool(Key.IS_COMPRESS, false);

            // 如果不是动态分区写入，则检查分区配置，动态分区写入不用检查
            if (!this.supportDynamicPartition) {
                this.partition = OdpsUtil.formatPartition(this.originalConfig
                        .getString(Key.PARTITION, ""), true);
                this.originalConfig.set(Key.PARTITION, this.partition);
            }

            this.truncate = this.originalConfig.getBool(Key.TRUNCATE);

            this.consistencyCommit = this.originalConfig.getBool(Key.CONSISTENCY_COMMIT, false);

            boolean emptyAsNull = this.originalConfig.getBool(Key.EMPTY_AS_NULL, false);
            this.originalConfig.set(Key.EMPTY_AS_NULL, emptyAsNull);
            if (emptyAsNull) {
                LOG.warn(MESSAGE_SOURCE.message("odpswriter.2"));
            }

            this.blockSizeInMB = this.originalConfig.getInt(Key.BLOCK_SIZE_IN_MB, 64);
            if (this.blockSizeInMB < 8) {
                this.blockSizeInMB = 8;
            }
            this.originalConfig.set(Key.BLOCK_SIZE_IN_MB, this.blockSizeInMB);
            LOG.info("blockSizeInMB={}.", this.blockSizeInMB);
            maxPartitionCnt = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax() / 1024 / 1024 / this.blockSizeInMB;
            if (maxPartitionCnt < Constant.MAX_PARTITION_CNT) {
                maxPartitionCnt = Constant.MAX_PARTITION_CNT;
            }
            LOG.info("maxPartitionCnt={}", maxPartitionCnt);

            if (IS_DEBUG) {
                LOG.debug("After master init(), job config now is: [\n{}\n] .",
                        this.originalConfig.toJSON());
            }
        }

        private void dealDynamicPartition() {
            /*
             * 如果显示配置了 supportDynamicPartition，则以配置为准
             * 如果没有配置，表为分区表且 列映射中包所有含分区列
             */
            List<String> partitionCols = OdpsUtil.getAllPartColumns(this.table.getSchema());
            List<String> configCols = this.originalConfig.getList(Key.COLUMN, String.class);
            LOG.info("partition columns:{}", partitionCols);
            LOG.info("config columns:{}", configCols);
            LOG.info("support dynamic partition:{}",this.originalConfig.getBool(Key.SUPPORT_DYNAMIC_PARTITION));
            LOG.info("partition format type:{}",this.originalConfig.getString("partitionFormatType"));
            if (this.originalConfig.getKeys().contains(Key.SUPPORT_DYNAMIC_PARTITION)) {
                this.supportDynamicPartition = this.originalConfig.getBool(Key.SUPPORT_DYNAMIC_PARTITION);
                if (supportDynamicPartition) {
                    // 自定义分区
                    if("custom".equalsIgnoreCase(originalConfig.getString("partitionFormatType"))){
                        List<PartitionInfo> partitions = getListWithJson(originalConfig,"customPartitionColumns",PartitionInfo.class);
                        // 自定义分区配置必须与实际分区列完全一致
                        if (!ListUtil.checkIfAllSameValue(partitions.stream().map(item->item.getName()).collect(Collectors.toList()), partitionCols)) {
                            throw DataXException.asDataXException("custom partition config is not same as real partition info.");
                        }
                    } else {
                        // 设置动态分区写入为真--检查是否所有分区列都配置在了列映射中，不满足则抛出异常
                        if (!ListUtil.checkIfBInA(configCols, partitionCols, false)) {
                            throw DataXException.asDataXException("You config supportDynamicPartition as true, but didn't config all partition columns");
                        }
                    }
                } else {
                    // 设置动态分区写入为假--确保列映射中没有配置分区列，配置则抛出异常
                    if (ListUtil.checkIfHasSameValue(configCols, partitionCols)) {
                        throw DataXException.asDataXException("You should config all partition columns in column param, or you can specify a static partition param");
                    }
                }
            } else {
                if (OdpsUtil.isPartitionedTable(table)) {
                    // 分区表，列映射配置了分区，同时检查所有分区列要么都被配置，要么都没有配置
                    if (ListUtil.checkIfBInA(configCols, partitionCols, false)) {
                        // 所有的partition 列都配置在了column中
                        this.supportDynamicPartition = true;
                    } else {
                        // 并非所有partition列都配置在了column中，此时还需检查是否只配置了部分，如果只配置了部分，则报错
                        if (ListUtil.checkIfHasSameValue(configCols, partitionCols)) {
                            throw DataXException.asDataXException("You should config all partition columns in column param, or you can specify a static partition param");
                        }
                        // 分区列没有配置任何分区列，则设置为false
                        this.supportDynamicPartition = false;
                    }
                } else {
                    LOG.info("{} is not a partition tale, set supportDynamicParition as false", this.tableName);
                    this.supportDynamicPartition = false;
                }
            }

            // 分布式下不支持动态分区写入，如果是分布式模式则报错
            LOG.info("current run mode: {}", System.getProperty("datax.executeMode"));
            if (supportDynamicPartition && StringUtils.equalsIgnoreCase("distribute", System.getProperty("datax.executeMode"))) {
                LOG.error("Distribute mode don't support dynamic partition writing");
                System.exit(1);
            }
        }

        @Override
        public void prepare() {
            // init odps config
            this.odps = OdpsUtil.initOdpsProject(this.originalConfig);

            List<String> preSqls = this.originalConfig.getList(Key.PRE_SQL, String.class);
            if (preSqls != null && !preSqls.isEmpty()) {
                LOG.info(String.format("Beigin to exectue preSql : %s. \n Attention: these preSqls must be idempotent!!!",
                        JSON.toJSONString(preSqls)));
                long beginTime = System.currentTimeMillis();
                for (String preSql : preSqls) {
                    preSql = preSql.trim();
                    if (!preSql.endsWith(";")) {
                        preSql = String.format("%s;", preSql);
                    }
                    OdpsUtil.runSqlTaskWithRetry(this.odps, preSql, "preSql");
                }
                long endTime = System.currentTimeMillis();
                LOG.info(String.format("Exectue odpswriter preSql successfully! cost time: %s ms.", (endTime - beginTime)));
            }

            //检查表等配置是否正确
            this.table = OdpsUtil.getTable(odps, this.projectName, this.tableName);

            // 如果是动态分区写入，因为无需配置分区信息，因此也无法在任务初始化时进行 truncate
            if (!supportDynamicPartition) {
                OdpsUtil.dealTruncate(this.odps, this.table, this.partition, this.truncate);
            }
        }

        /**
         * 此处主要是对 uploadId进行设置，以及对 blockId 的开始值进行设置。
         * <p/>
         * 对 blockId 需要同时设置开始值以及下一个 blockId 的步长值(INTERVAL_STEP)。
         */
        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configurations = new ArrayList<Configuration>();

            // 此处获取到 masterUpload 只是为了拿到 RecordSchema,以完成对 column 的处理
            TableTunnel tableTunnel = new TableTunnel(this.odps);
            if (StringUtils.isNoneBlank(tunnelServer)) {
                tableTunnel.setEndpoint(tunnelServer);
            }

            TableSchema schema = this.table.getSchema();
            List<String> allColumns = OdpsUtil.getAllColumns(schema);
            LOG.info("allColumnList: {} .", StringUtils.join(allColumns, ','));
            List<String> allPartColumns = OdpsUtil.getAllPartColumns(this.table.getSchema());
            LOG.info("allPartColumnsList: {} .", StringUtils.join(allPartColumns, ','));
            dealColumn(this.originalConfig, allColumns, allPartColumns);
            this.originalConfig.set("allColumns", allColumns);

            // 动态分区模式下，无法事先根据分区创建好 session,
            if (!supportDynamicPartition) {
                this.masterUpload = OdpsUtil.createMasterTunnelUpload(
                        tableTunnel, this.projectName, this.tableName, this.partition);
                this.uploadId = this.masterUpload.getId();
                LOG.info("Master uploadId:[{}].", this.uploadId);
            }

            for (int i = 0; i < mandatoryNumber; i++) {
                Configuration tempConfig = this.originalConfig.clone();

                // 非动态分区模式下，设置了统一提交，则需要克隆主 upload session，否则各个 task "各自为战"
                if (!supportDynamicPartition && this.consistencyCommit) {
                    tempConfig.set(Key.UPLOAD_ID, uploadId);
                    tempConfig.set(Key.TASK_COUNT, mandatoryNumber);
                }

                // 设置task的supportDynamicPartition属性
                tempConfig.set(Key.SUPPORT_DYNAMIC_PARTITION, this.supportDynamicPartition);

                configurations.add(tempConfig);
            }

            if (IS_DEBUG) {
                LOG.debug("After master split, the job config now is:[\n{}\n].", this.originalConfig);
            }

            return configurations;
        }

        private void dealColumn(Configuration originalConfig, List<String> allColumns, List<String> allPartColumns) {
            //之前已经检查了userConfiguredColumns 一定不为空
            List<String> userConfiguredColumns = originalConfig.getList(Key.COLUMN, String.class);

            // 动态分区下column不支持配置*
            if (supportDynamicPartition && userConfiguredColumns.contains("*")) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.ILLEGAL_VALUE,
                        "In dynamic partition write mode you can't specify column with *.");
            }
            if (1 == userConfiguredColumns.size() && "*".equals(userConfiguredColumns.get(0))) {
                userConfiguredColumns = allColumns;
                originalConfig.set(Key.COLUMN, allColumns);
            } else {
                //检查列是否重复，大小写不敏感（所有写入，都是不允许写入段的列重复的）
                ListUtil.makeSureNoValueDuplicate(userConfiguredColumns, false);

                //检查列是否存在，大小写不敏感
                if (supportDynamicPartition) {
                    List<String> allColumnList = new ArrayList<String>();
                    allColumnList.addAll(allColumns);
                    allColumnList.addAll(allPartColumns);
                    ListUtil.makeSureBInA(allColumnList, userConfiguredColumns, false);
                } else {
                    ListUtil.makeSureBInA(allColumns, userConfiguredColumns, false);
                }
            }

            // 获取配置的所有数据列在目标表中所有数据列中的真正位置, -1 代表该列为分区列
            List<Integer> columnPositions = OdpsUtil.parsePosition(allColumns, allPartColumns, userConfiguredColumns);
            originalConfig.set(Constant.COLUMN_POSITION, columnPositions);
        }

        @Override
        public void post() {

            if (supportDynamicPartition) {
                LOG.info("Total create partition cnt:{}", partitionCnt);
            }

            if (!supportDynamicPartition && this.consistencyCommit) {
                LOG.info("Master which uploadId=[{}] begin to commit blocks.", this.uploadId);
                OdpsUtil.masterComplete(this.masterUpload);
                LOG.info("Master which uploadId=[{}] commit blocks ok.", this.uploadId);
            }

            List<String> postSqls = this.originalConfig.getList(Key.POST_SQL, String.class);
            if (postSqls != null && !postSqls.isEmpty()) {
                LOG.info(String.format("Beigin to exectue postSql : %s. \n Attention: these postSqls must be idempotent!!!",
                        JSON.toJSONString(postSqls)));
                long beginTime = System.currentTimeMillis();
                for (String postSql : postSqls) {
                    postSql = postSql.trim();
                    if (!postSql.endsWith(";")) {
                        postSql = String.format("%s;", postSql);
                    }
                    OdpsUtil.runSqlTaskWithRetry(this.odps, postSql, "postSql");
                }
                long endTime = System.currentTimeMillis();
                LOG.info(String.format("Exectue odpswriter postSql successfully! cost time: %s ms.", (endTime - beginTime)));
            }

            LOG.info("truncated record count: {}", globalTotalTruncatedRecordNumber.intValue() );
        }

        @Override
        public void destroy() {
        }
    }


    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);
        private static final MessageSource MESSAGE_SOURCE = MessageSource.loadResourceBundle(OdpsWriter.class);

        private static final boolean IS_DEBUG = LOG.isDebugEnabled();

        private Configuration sliceConfig;
        private Odps odps;

        private String projectName;
        private String tableName;
        private String tunnelServer;
        private String partition;
        private boolean emptyAsNull;
        private boolean isCompress;

        private TableTunnel.UploadSession managerUpload;
        private TableTunnel.UploadSession workerUpload;

        private String uploadId = null;
        private List<Long> blocks;
        private int blockSizeInMB;

        private boolean consistencyCommit;

        private int taskId;
        private int taskCount;

        private Integer failoverState = 0; //0 未failover 1准备failover 2已提交，不能failover
        private byte[] lock = new byte[0];
        private List<String> allColumns;

        /*
         * Partition 和 session 的对应关系，处理 record 时，路由到哪个分区，则通过对应的 proxy 上传
         * Key 为 所有分区列的值按配置顺序拼接
         */
        private HashMap</*partition*/String, Pair<OdpsWriterProxy, /*blocks*/List<Long>>> partitionUploadSessionHashMap;
        private Boolean supportDynamicPartition;
        private TableTunnel tableTunnel;
        private Table table;

        /**
         * 保存分区列格式转换规则，只支持源表是 Date 列，或者内容为日期的 String 列
         */
        private HashMap<String, DateTransForm> dateTransFormMap;

        private Long writeTimeOutInMs;

        private String overLengthRule;
        private int maxFieldLength;
        private Boolean enableOverLengthOutput;

        /**
         * 动态分区写入模式下，内存使用率达到80%则flush时间间隔，单位分钟
         * 默认5分钟做flush, 避免出现频繁的flush导致小文件问题
         */
        private int dynamicPartitionMemUsageFlushIntervalInMinute = 1;

        private long latestFlushTime = 0;

        @Override
        public void init() {
            this.sliceConfig = super.getPluginJobConf();

            // 默认十分钟超时时间
            this.writeTimeOutInMs = this.sliceConfig.getLong(Key.WRITE_TIMEOUT_IN_MS, 10 * 60 * 1000);
            this.projectName = this.sliceConfig.getString(Key.PROJECT);
            this.tableName = this.sliceConfig.getString(Key.TABLE);
            this.tunnelServer = this.sliceConfig.getString(Key.TUNNEL_SERVER, null);
            this.partition = OdpsUtil.formatPartition(this.sliceConfig
                    .getString(Key.PARTITION, ""), true);
            this.sliceConfig.set(Key.PARTITION, this.partition);

            this.emptyAsNull = this.sliceConfig.getBool(Key.EMPTY_AS_NULL);
            this.blockSizeInMB = this.sliceConfig.getInt(Key.BLOCK_SIZE_IN_MB);
            this.isCompress = this.sliceConfig.getBool(Key.IS_COMPRESS, false);
            if (this.blockSizeInMB < 1 || this.blockSizeInMB > 512) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.ILLEGAL_VALUE,
                        MESSAGE_SOURCE.message("odpswriter.3", this.blockSizeInMB));
            }

            this.taskId = this.getTaskId();
            this.taskCount = this.sliceConfig.getInt(Key.TASK_COUNT, 0);

            this.supportDynamicPartition = this.sliceConfig.getBool(Key.SUPPORT_DYNAMIC_PARTITION, false);

            if (!supportDynamicPartition) {
                this.consistencyCommit = this.sliceConfig.getBool(Key.CONSISTENCY_COMMIT, false);
                if (consistencyCommit) {
                    this.uploadId = this.sliceConfig.getString(Key.UPLOAD_ID);
                    if (this.uploadId == null || this.uploadId.isEmpty()) {
                        throw DataXException.asDataXException(OdpsWriterErrorCode.ILLEGAL_VALUE,
                                MESSAGE_SOURCE.message("odpswriter.3", this.uploadId));
                    }
                }
            } else {
                this.partitionUploadSessionHashMap = new HashMap<>();

                // 根据 partColFormats 参数初始化 dateTransFormMap
                String dateTransListStr = this.sliceConfig.getString(Key.PARTITION_COL_MAPPING);
                if (StringUtils.isNotBlank(dateTransListStr)) {
                    this.dateTransFormMap = new HashMap<>();
                    JSONArray dateTransFormJsonArray = JSONArray.parseArray(dateTransListStr);
                    for (Object dateTransFormJson : dateTransFormJsonArray) {
                        DateTransForm dateTransForm = new DateTransForm(
                                ((JSONObject)dateTransFormJson).getString(Key.PARTITION_COL_MAPPING_NAME),
                                ((JSONObject)dateTransFormJson).getString(Key.PARTITION_COL_MAPPING_SRC_COL_DATEFORMAT),
                                ((JSONObject)dateTransFormJson).getString(Key.PARTITION_COL_MAPPING_DATEFORMAT));
                        this.dateTransFormMap.put(((JSONObject)dateTransFormJson).getString(Key.PARTITION_COL_MAPPING_NAME), dateTransForm);
                    }
                }
            }
            this.allColumns = this.sliceConfig.getList("allColumns", String.class);
            this.overLengthRule = this.sliceConfig.getString(Key.OVER_LENGTH_RULE, "keepOn").toUpperCase();
            this.maxFieldLength = this.sliceConfig.getInt(Key.MAX_FIELD_LENGTH, Constant.DEFAULT_FIELD_MAX_SIZE);
            this.enableOverLengthOutput = this.sliceConfig.getBool(Key.ENABLE_OVER_LENGTH_OUTPUT, true);
            maxOutputOverLengthRecord = this.sliceConfig.getLong(Key.MAX_OVER_LENGTH_OUTPUT_COUNT);
            maxOdpsFieldLength = this.sliceConfig.getInt(Key.MAX_ODPS_FIELD_LENGTH, Constant.DEFAULT_FIELD_MAX_SIZE);

            this.dynamicPartitionMemUsageFlushIntervalInMinute = this.sliceConfig.getInt(Key.DYNAMIC_PARTITION_MEM_USAGE_FLUSH_INTERVAL_IN_MINUTE,
                1);
            if (IS_DEBUG) {
                LOG.debug("After init in task, sliceConfig now is:[\n{}\n].", this.sliceConfig);
            }

        }

        @Override
        public void prepare() {
            this.odps = OdpsUtil.initOdpsProject(this.sliceConfig);
            this.tableTunnel = new TableTunnel(this.odps);

            if (! supportDynamicPartition ) {
                if (StringUtils.isNoneBlank(tunnelServer)) {
                    tableTunnel.setEndpoint(tunnelServer);
                }
                if (this.consistencyCommit) {
                    this.managerUpload = OdpsUtil.getSlaveTunnelUpload(this.tableTunnel, this.projectName, this.tableName,
                            this.partition, this.uploadId);
                } else {
                    this.managerUpload = OdpsUtil.createMasterTunnelUpload(this.tableTunnel, this.projectName,
                            this.tableName, this.partition);
                    this.uploadId = this.managerUpload.getId();
                }
                LOG.info("task uploadId:[{}].", this.uploadId);
                this.workerUpload = OdpsUtil.getSlaveTunnelUpload(this.tableTunnel, this.projectName,
                        this.tableName, this.partition, uploadId);
            } else {
                this.table = OdpsUtil.getTable(this.odps, this.projectName, this.tableName);
            }
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            blocks = new ArrayList<Long>();
            List<Long> currentWriteBlocks;

            AtomicLong blockId = new AtomicLong(0);

            List<Integer> columnPositions = this.sliceConfig.getList(Constant.COLUMN_POSITION,
                    Integer.class);

            try {
                TaskPluginCollector taskPluginCollector = super.getTaskPluginCollector();

                OdpsWriterProxy proxy;
                // 可以配置化，保平安
                boolean checkWithGetSize = this.sliceConfig.getBool("checkWithGetSize", true);
                if (!supportDynamicPartition) {
                    if (this.consistencyCommit) {
                        proxy = new OdpsWriterProxy(this.workerUpload, this.blockSizeInMB, blockId, taskId, taskCount,
                                columnPositions, taskPluginCollector, this.emptyAsNull, this.isCompress, checkWithGetSize, this.allColumns, this.writeTimeOutInMs, this.sliceConfig, this.overLengthRule, this.maxFieldLength, this.enableOverLengthOutput);
                    } else {
                        proxy = new OdpsWriterProxy(this.workerUpload, this.blockSizeInMB, blockId,
                                columnPositions, taskPluginCollector, this.emptyAsNull, this.isCompress, checkWithGetSize, this.allColumns, false, this.writeTimeOutInMs, this.sliceConfig, this.overLengthRule, this.maxFieldLength, this.enableOverLengthOutput);
                    }
                    currentWriteBlocks = blocks;
                } else {
                    proxy = null;
                    currentWriteBlocks = null;
                }

                com.alibaba.datax.common.element.Record dataXRecord = null;

                PerfRecord blockClose = new PerfRecord(super.getTaskGroupId(), super.getTaskId(), PerfRecord.PHASE.ODPS_BLOCK_CLOSE);
                blockClose.start();
                long blockCloseUsedTime = 0;
                boolean columnCntChecked = false;
                while ((dataXRecord = recordReceiver.getFromReader()) != null) {
                    if (supportDynamicPartition) {
                        if (!columnCntChecked) {
                            // 动态分区模式下，读写两端的column数量必须相同
                            if (dataXRecord.getColumnNumber() != this.sliceConfig.getList(Key.COLUMN).size()) {
                                throw DataXException.asDataXException(OdpsWriterErrorCode.ILLEGAL_VALUE,
                                        "In dynamic partition write mode you must make sure reader and writer has same column count.");
                            }
                            columnCntChecked = true;
                        }

                        // 如果是动态分区模式，则需要根据record内容来选择proxy

                        String partitionFormatType = sliceConfig.getString("partitionFormatType");
                        String partition;
                        if("custom".equalsIgnoreCase(partitionFormatType)){
                            List<PartitionInfo> partitions = getListWithJson(sliceConfig,"customPartitionColumns",PartitionInfo.class);
                            List<UserDefinedFunction> functions = getListWithJson(sliceConfig,"customPartitionFunctions",UserDefinedFunction.class);

                            partition = CustomPartitionUtils.generate(dataXRecord,functions,
                                    partitions,sliceConfig.getList(Key.COLUMN, String.class));
                        }else{
                            partition = OdpsUtil.getPartColValFromDataXRecord(dataXRecord, columnPositions,
                                    this.sliceConfig.getList(Key.COLUMN, String.class),
                                    this.dateTransFormMap);
                            partition = OdpsUtil.formatPartition(partition, false);
                        }

                        Pair<OdpsWriterProxy, List<Long>> proxyBlocksPair = this.partitionUploadSessionHashMap.get(partition);
                        if (null != proxyBlocksPair) {
                            proxy = proxyBlocksPair.getLeft();
                            currentWriteBlocks = proxyBlocksPair.getRight();
                            if (null == proxy || null == currentWriteBlocks) {
                                throw DataXException.asDataXException("Get OdpsWriterProxy failed.");
                            }
                        } else {
                            /*
                             * 第一次写入该目标分区：处理truncate
                             * truncate 为 true，且还没有被truncate过，则truncate，加互斥锁
                             */
                            Boolean truncate = this.sliceConfig.getBool(Key.TRUNCATE);
                            if (truncate && !partitionsDealedTruncate.contains(partition)) {
                                synchronized (lockForPartitionDealedTruncate) {
                                    if (!partitionsDealedTruncate.contains(partition)) {
                                        LOG.info("Start to truncate partition {}", partition);
                                        OdpsUtil.dealTruncate(this.odps, this.table, partition, truncate);
                                        partitionsDealedTruncate.add(partition);
                                    }
                                /*
                                 * 判断分区是否创建过多，如果创建过多，则报错
                                 */
                                    if (partitionCnt.addAndGet(1) > maxPartitionCnt) {
                                        throw new DataXException("Create too many partitions. Please make sure you config the right partition column");
                                    }
                                }
                            }
                            TableTunnel.UploadSession uploadSession = OdpsUtil.createMasterTunnelUpload(tableTunnel, this.projectName,
                                    this.tableName, partition);
                            proxy = new OdpsWriterProxy(uploadSession, this.blockSizeInMB, blockId,
                                    columnPositions, taskPluginCollector, this.emptyAsNull, this.isCompress, checkWithGetSize, this.allColumns, true, this.writeTimeOutInMs, this.sliceConfig, this.overLengthRule, this.maxFieldLength, this.enableOverLengthOutput);
                            currentWriteBlocks = new ArrayList<>();
                            partitionUploadSessionHashMap.put(partition, new MutablePair<>(proxy, currentWriteBlocks));
                        }
                    }
                    blockCloseUsedTime += proxy.writeOneRecord(dataXRecord, currentWriteBlocks);

                    // 动态分区写入模式下，如果内存使用达到一定程度 80%，清理较久没有活动且缓存较多数据的分区
                    if (supportDynamicPartition) {
                        boolean isNeedFush = checkIfNeedFlush();
                        if (isNeedFush) {
                            LOG.info("====The memory used exceed 80%, start to clear...===");
                            int releaseCnt = 0;
                            int remainCnt = 0;
                            for (String onePartition : partitionUploadSessionHashMap.keySet()) {
                                OdpsWriterProxy oneIdleProxy = partitionUploadSessionHashMap.get(onePartition) == null ? null : partitionUploadSessionHashMap.get(onePartition).getLeft();
                                if (oneIdleProxy == null) {
                                    continue;
                                }

                                Long idleTime = System.currentTimeMillis() - oneIdleProxy.getLastActiveTime();
                                if (idleTime > Constant.PROXY_MAX_IDLE_TIME_MS || oneIdleProxy.getCurrentTotalBytes() > (this.blockSizeInMB*1014*1024 / 2)) {
                                    // 如果空闲一定时间，先把数据写出
                                    LOG.info("{} partition has no data last {} seconds, so release its uploadSession", onePartition, Constant.PROXY_MAX_IDLE_TIME_MS / 1000);
                                    currentWriteBlocks = partitionUploadSessionHashMap.get(onePartition).getRight();
                                    blockCloseUsedTime += oneIdleProxy.writeRemainingRecord(currentWriteBlocks);
                                    // 再清除
                                    partitionUploadSessionHashMap.put(onePartition, null);
                                    releaseCnt++;
                                } else {
                                    remainCnt++;
                                }
                            }

                            // 释放的不足够多，再释放一次，这次随机释放，直到释放数量达到一半
                            for (String onePartition : partitionUploadSessionHashMap.keySet()) {
                                if (releaseCnt >= remainCnt) {
                                    break;
                                }

                                if (partitionUploadSessionHashMap.get(onePartition) != null) {
                                    OdpsWriterProxy oneIdleProxy = partitionUploadSessionHashMap.get(onePartition).getLeft();
                                    currentWriteBlocks = partitionUploadSessionHashMap.get(onePartition).getRight();
                                    blockCloseUsedTime += oneIdleProxy.writeRemainingRecord(currentWriteBlocks);
                                    partitionUploadSessionHashMap.put(onePartition, null);

                                    releaseCnt++;
                                    remainCnt--;
                                }

                            }

                            this.latestFlushTime = System.currentTimeMillis();
                            LOG.info("===complete===");
                        }

                    }
                }

                // 对所有分区进行剩余 records 写入
                if (supportDynamicPartition) {
                    for (String partition : partitionUploadSessionHashMap.keySet()) {
                        if (partitionUploadSessionHashMap.get(partition) == null) {
                            continue;
                        }
                        proxy = partitionUploadSessionHashMap.get(partition).getLeft();
                        currentWriteBlocks = partitionUploadSessionHashMap.get(partition).getRight();
                        blockCloseUsedTime += proxy.writeRemainingRecord(currentWriteBlocks);
                        blockClose.end(blockCloseUsedTime);
                    }
                }
                else {
                    blockCloseUsedTime += proxy.writeRemainingRecord(blocks);
                    blockClose.end(blockCloseUsedTime);
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.WRITER_RECORD_FAIL, MESSAGE_SOURCE.message("odpswriter.4"), e);
            }
        }

        private boolean checkIfNeedFlush() {

            //检查是否到达flush时间，超过flush间隔时间
            boolean isArriveFlushTime = (System.currentTimeMillis() - this.latestFlushTime) > this.dynamicPartitionMemUsageFlushIntervalInMinute * 60 * 1000;
            if (!isArriveFlushTime) {
                //如果flush时间没有到，直接return掉
                return false;
            }

            MemoryUsage memoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
            boolean isMemUsageExceed = (double)memoryUsage.getUsed() / memoryUsage.getMax() > 0.8f;
            return isMemUsageExceed;
        }

        @Override
        public void post() {
            synchronized (lock) {
                if (failoverState == 0) {
                    failoverState = 2;
                    if (! supportDynamicPartition) {
                        if (! this.consistencyCommit) {
                            LOG.info("Slave which uploadId=[{}] begin to commit blocks:[\n{}\n].", this.uploadId,
                                    StringUtils.join(blocks, ","));
                            OdpsUtil.masterCompleteBlocks(this.managerUpload, blocks.toArray(new Long[0]));
                            LOG.info("Slave which uploadId=[{}] commit blocks ok.", this.uploadId);
                        } else {
                            LOG.info("Slave which uploadId=[{}] begin to check blocks:[\n{}\n].", this.uploadId,
                                    StringUtils.join(blocks, ","));
                            OdpsUtil.checkBlockComplete(this.managerUpload, blocks.toArray(new Long[0]));
                            LOG.info("Slave which uploadId=[{}] check blocks ok.", this.uploadId);
                        }
                    } else {
                        for (String partition : partitionUploadSessionHashMap.keySet()) {
                            OdpsWriterProxy proxy = partitionUploadSessionHashMap.get(partition).getLeft();
                            List<Long> blocks = partitionUploadSessionHashMap.get(partition).getRight();
                            TableTunnel.UploadSession uploadSession = proxy.getSlaveUpload();
                            LOG.info("Slave which uploadId=[{}] begin to check blocks:[\n{}\n].", uploadSession.getId(),
                                    StringUtils.join(blocks, ","));
                            OdpsUtil.masterCompleteBlocks(uploadSession, blocks.toArray(new Long[0]));
                            LOG.info("Slave which uploadId=[{}] check blocks ok.", uploadSession.getId());
                        }
                    }

                } else {
                    throw DataXException.asDataXException(CommonErrorCode.SHUT_DOWN_TASK, "");
                }
            }
        }

        @Override
        public void destroy() {
        }

        @Override
        public boolean supportFailOver() {
            synchronized (lock) {
                if (failoverState == 0) {
                    failoverState = 1;
                    return true;
                }
                return false;
            }
        }
    }
}