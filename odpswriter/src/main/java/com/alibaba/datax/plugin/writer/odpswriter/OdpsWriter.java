package com.alibaba.datax.plugin.writer.odpswriter;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.ListUtil;
import com.alibaba.datax.plugin.writer.odpswriter.util.IdAndKeyUtil;
import com.alibaba.datax.plugin.writer.odpswriter.util.OdpsUtil;

import com.aliyun.odps.Odps;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.tunnel.TableTunnel;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 已修改为：每个 task 各自创建自己的 upload,拥有自己的 uploadId，并在 task 中完成对对应 block 的提交。
 */
public class OdpsWriter extends Writer {

    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private static final boolean IS_DEBUG = LOG.isDebugEnabled();

        private Configuration originalConfig;
        private Odps odps;
        private Table table;

        private String projectName;
        private String tableName;
        private String tunnelServer;
        private String partition;
        private String accountType;
        private boolean truncate;
        private String uploadId;
        private TableTunnel.UploadSession masterUpload;
        private int blockSizeInMB;

        public void preCheck() {
            this.init();
            this.doPreCheck();
        }

        public void doPreCheck() {
            //检查accessId,accessKey配置
            if (Constant.DEFAULT_ACCOUNT_TYPE
                    .equalsIgnoreCase(this.accountType)) {
                this.originalConfig = IdAndKeyUtil.parseAccessIdAndKey(this.originalConfig);
                String accessId = this.originalConfig.getString(Key.ACCESS_ID);
                String accessKey = this.originalConfig.getString(Key.ACCESS_KEY);
                if (IS_DEBUG) {
                    LOG.debug("accessId:[{}], accessKey:[{}] .", accessId,
                            accessKey);
                }
                LOG.info("accessId:[{}] .", accessId);
            }
            // init odps config
            this.odps = OdpsUtil.initOdpsProject(this.originalConfig);

            //检查表等配置是否正确
            this.table = OdpsUtil.getTable(odps,this.projectName,this.tableName);

            //检查列信息是否正确
            List<String> allColumns = OdpsUtil.getAllColumns(this.table.getSchema());
            LOG.info("allColumnList: {} .", StringUtils.join(allColumns, ','));
            dealColumn(this.originalConfig, allColumns);

            //检查分区信息是否正确
            OdpsUtil.preCheckPartition(this.odps, this.table, this.partition, this.truncate);
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            OdpsUtil.checkNecessaryConfig(this.originalConfig);
            OdpsUtil.dealMaxRetryTime(this.originalConfig);

            this.projectName = this.originalConfig.getString(Key.PROJECT);
            this.tableName = this.originalConfig.getString(Key.TABLE);
            this.tunnelServer = this.originalConfig.getString(Key.TUNNEL_SERVER, null);

            //check isCompress
            this.originalConfig.getBool(Key.IS_COMPRESS, false);

            this.partition = OdpsUtil.formatPartition(this.originalConfig
                    .getString(Key.PARTITION, ""));
            this.originalConfig.set(Key.PARTITION, this.partition);

            this.accountType = this.originalConfig.getString(Key.ACCOUNT_TYPE,
                    Constant.DEFAULT_ACCOUNT_TYPE);
            if (!Constant.DEFAULT_ACCOUNT_TYPE.equalsIgnoreCase(this.accountType) &&
                    !Constant.TAOBAO_ACCOUNT_TYPE.equalsIgnoreCase(this.accountType)) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.ACCOUNT_TYPE_ERROR,
                        String.format("账号类型错误，因为你的账号 [%s] 不是datax目前支持的账号类型，目前仅支持aliyun, taobao账号，请修改您的账号信息.", accountType));
            }
            this.originalConfig.set(Key.ACCOUNT_TYPE, this.accountType);

            this.truncate = this.originalConfig.getBool(Key.TRUNCATE);

            boolean emptyAsNull = this.originalConfig.getBool(Key.EMPTY_AS_NULL, false);
            this.originalConfig.set(Key.EMPTY_AS_NULL, emptyAsNull);
            if (emptyAsNull) {
                LOG.warn("这是一条需要注意的信息 由于您的作业配置了写入 ODPS 的目的表时emptyAsNull=true, 所以 DataX将会把长度为0的空字符串作为 java 的 null 写入 ODPS.");
            }

            this.blockSizeInMB = this.originalConfig.getInt(Key.BLOCK_SIZE_IN_MB, 64);
            if(this.blockSizeInMB < 8) {
                this.blockSizeInMB = 8;
            }
            this.originalConfig.set(Key.BLOCK_SIZE_IN_MB, this.blockSizeInMB);
            LOG.info("blockSizeInMB={}.", this.blockSizeInMB);

            if (IS_DEBUG) {
                LOG.debug("After master init(), job config now is: [\n{}\n] .",
                        this.originalConfig.toJSON());
            }
        }

        @Override
        public void prepare() {
            String accessId = null;
            String accessKey = null;
            if (Constant.DEFAULT_ACCOUNT_TYPE
                    .equalsIgnoreCase(this.accountType)) {
                this.originalConfig = IdAndKeyUtil.parseAccessIdAndKey(this.originalConfig);
                accessId = this.originalConfig.getString(Key.ACCESS_ID);
                accessKey = this.originalConfig.getString(Key.ACCESS_KEY);
                if (IS_DEBUG) {
                    LOG.debug("accessId:[{}], accessKey:[{}] .", accessId,
                            accessKey);
                }
                LOG.info("accessId:[{}] .", accessId);
            }

            // init odps config
            this.odps = OdpsUtil.initOdpsProject(this.originalConfig);

            //检查表等配置是否正确
            this.table = OdpsUtil.getTable(odps,this.projectName,this.tableName);

            OdpsUtil.dealTruncate(this.odps, this.table, this.partition, this.truncate);
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

            this.masterUpload = OdpsUtil.createMasterTunnelUpload(
                    tableTunnel, this.projectName, this.tableName, this.partition);
            this.uploadId = this.masterUpload.getId();
            LOG.info("Master uploadId:[{}].", this.uploadId);

            TableSchema schema = this.masterUpload.getSchema();
            List<String> allColumns = OdpsUtil.getAllColumns(schema);
            LOG.info("allColumnList: {} .", StringUtils.join(allColumns, ','));

            dealColumn(this.originalConfig, allColumns);

            for (int i = 0; i < mandatoryNumber; i++) {
                Configuration tempConfig = this.originalConfig.clone();

                configurations.add(tempConfig);
            }

            if (IS_DEBUG) {
                LOG.debug("After master split, the job config now is:[\n{}\n].", this.originalConfig);
            }

            this.masterUpload = null;

            return configurations;
        }

        private void dealColumn(Configuration originalConfig, List<String> allColumns) {
            //之前已经检查了userConfiguredColumns 一定不为空
            List<String> userConfiguredColumns = originalConfig.getList(Key.COLUMN, String.class);
            if (1 == userConfiguredColumns.size() && "*".equals(userConfiguredColumns.get(0))) {
                userConfiguredColumns = allColumns;
                originalConfig.set(Key.COLUMN, allColumns);
            } else {
                //检查列是否重复，大小写不敏感（所有写入，都是不允许写入段的列重复的）
                ListUtil.makeSureNoValueDuplicate(userConfiguredColumns, false);

                //检查列是否存在，大小写不敏感
                ListUtil.makeSureBInA(allColumns, userConfiguredColumns, false);
            }

            List<Integer> columnPositions = OdpsUtil.parsePosition(allColumns, userConfiguredColumns);
            originalConfig.set(Constant.COLUMN_POSITION, columnPositions);
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }


    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);

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

        private Integer failoverState = 0; //0 未failover 1准备failover 2已提交，不能failover
        private byte[] lock = new byte[0];

        @Override
        public void init() {
            this.sliceConfig = super.getPluginJobConf();

            this.projectName = this.sliceConfig.getString(Key.PROJECT);
            this.tableName = this.sliceConfig.getString(Key.TABLE);
            this.tunnelServer = this.sliceConfig.getString(Key.TUNNEL_SERVER, null);
            this.partition = OdpsUtil.formatPartition(this.sliceConfig
                    .getString(Key.PARTITION, ""));
            this.sliceConfig.set(Key.PARTITION, this.partition);

            this.emptyAsNull = this.sliceConfig.getBool(Key.EMPTY_AS_NULL);
            this.blockSizeInMB = this.sliceConfig.getInt(Key.BLOCK_SIZE_IN_MB);
            this.isCompress = this.sliceConfig.getBool(Key.IS_COMPRESS, false);
            if (this.blockSizeInMB < 1 || this.blockSizeInMB > 512) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.ILLEGAL_VALUE,
                        String.format("您配置的blockSizeInMB:%s 参数错误. 正确的配置是[1-512]之间的整数. 请修改此参数的值为该区间内的数值", this.blockSizeInMB));
            }

            if (IS_DEBUG) {
                LOG.debug("After init in task, sliceConfig now is:[\n{}\n].", this.sliceConfig);
            }

        }

        @Override
        public void prepare() {
            this.odps = OdpsUtil.initOdpsProject(this.sliceConfig);

            TableTunnel tableTunnel = new TableTunnel(this.odps);
            if (StringUtils.isNoneBlank(tunnelServer)) {
                tableTunnel.setEndpoint(tunnelServer);
            }

            this.managerUpload = OdpsUtil.createMasterTunnelUpload(tableTunnel, this.projectName,
                    this.tableName, this.partition);
            this.uploadId = this.managerUpload.getId();
            LOG.info("task uploadId:[{}].", this.uploadId);

            this.workerUpload = OdpsUtil.getSlaveTunnelUpload(tableTunnel, this.projectName,
                    this.tableName, this.partition, uploadId);
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            blocks = new ArrayList<Long>();

            AtomicLong blockId = new AtomicLong(0);

            List<Integer> columnPositions = this.sliceConfig.getList(Constant.COLUMN_POSITION,
                    Integer.class);

            try {
                TaskPluginCollector taskPluginCollector = super.getTaskPluginCollector();

                OdpsWriterProxy proxy = new OdpsWriterProxy(this.workerUpload, this.blockSizeInMB, blockId,
                        columnPositions, taskPluginCollector, this.emptyAsNull, this.isCompress);

                com.alibaba.datax.common.element.Record dataXRecord = null;

                PerfRecord blockClose = new PerfRecord(super.getTaskGroupId(),super.getTaskId(), PerfRecord.PHASE.ODPS_BLOCK_CLOSE);
                blockClose.start();
                long blockCloseUsedTime = 0;
                while ((dataXRecord = recordReceiver.getFromReader()) != null) {
                    blockCloseUsedTime += proxy.writeOneRecord(dataXRecord, blocks);
                }

                blockCloseUsedTime += proxy.writeRemainingRecord(blocks);
                blockClose.end(blockCloseUsedTime);
            } catch (Exception e) {
                throw DataXException.asDataXException(OdpsWriterErrorCode.WRITER_RECORD_FAIL, "写入 ODPS 目的表失败. 请联系 ODPS 管理员处理.", e);
            }
        }

        @Override
        public void post() {
            synchronized (lock){
                if(failoverState==0){
                    failoverState = 2;
                    LOG.info("Slave which uploadId=[{}] begin to commit blocks:[\n{}\n].", this.uploadId,
                            StringUtils.join(blocks, ","));
                    OdpsUtil.masterCompleteBlocks(this.managerUpload, blocks.toArray(new Long[0]));
                    LOG.info("Slave which uploadId=[{}] commit blocks ok.", this.uploadId);
                }else{
                    throw DataXException.asDataXException(CommonErrorCode.SHUT_DOWN_TASK, "");
                }
            }
        }

        @Override
        public void destroy() {
        }

        @Override
        public boolean supportFailOver(){
            synchronized (lock){
                if(failoverState==0){
                    failoverState = 1;
                    return true;
                }
                return false;
            }
        }
    }
}