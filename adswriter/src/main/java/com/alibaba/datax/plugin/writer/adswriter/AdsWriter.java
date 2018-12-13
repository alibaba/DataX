package com.alibaba.datax.plugin.writer.adswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.util.WriterUtil;
import com.alibaba.datax.plugin.writer.adswriter.ads.ColumnInfo;
import com.alibaba.datax.plugin.writer.adswriter.ads.TableInfo;
import com.alibaba.datax.plugin.writer.adswriter.insert.AdsClientProxy;
import com.alibaba.datax.plugin.writer.adswriter.insert.AdsInsertProxy;
import com.alibaba.datax.plugin.writer.adswriter.insert.AdsInsertUtil;
import com.alibaba.datax.plugin.writer.adswriter.insert.AdsProxy;
import com.alibaba.datax.plugin.writer.adswriter.load.AdsHelper;
import com.alibaba.datax.plugin.writer.adswriter.load.TableMetaHelper;
import com.alibaba.datax.plugin.writer.adswriter.load.TransferProjectConf;
import com.alibaba.datax.plugin.writer.adswriter.odps.TableMeta;
import com.alibaba.datax.plugin.writer.adswriter.util.AdsUtil;
import com.alibaba.datax.plugin.writer.adswriter.util.Constant;
import com.alibaba.datax.plugin.writer.adswriter.util.Key;
import com.alibaba.datax.plugin.writer.odpswriter.OdpsWriter;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.task.SQLTask;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AdsWriter extends Writer {

    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Writer.Job.class);
        public final static String ODPS_READER = "odpsreader";

        private OdpsWriter.Job odpsWriterJobProxy = new OdpsWriter.Job();
        private Configuration originalConfig;
        private Configuration readerConfig;

        /**
         * 持有ads账号的ads helper
         */
        private AdsHelper adsHelper;
        /**
         * 持有odps账号的ads helper
         */
        private AdsHelper odpsAdsHelper;
        /**
         * 中转odps的配置，对应到writer配置的parameter.odps部分
         */
        private TransferProjectConf transProjConf;
        private final int ODPSOVERTIME = 120000;
        private String odpsTransTableName;

        private String writeMode;
        private long startTime;

        @Override
        public void init() {
            startTime = System.currentTimeMillis();
            this.originalConfig = super.getPluginJobConf();
            this.writeMode = this.originalConfig.getString(Key.WRITE_MODE);
            if(null == this.writeMode) {
                LOG.warn("您未指定[writeMode]参数,  默认采用load模式, load模式只能用于离线表");
                this.writeMode = Constant.LOADMODE;
                this.originalConfig.set(Key.WRITE_MODE, "load");
            }

            if(Constant.LOADMODE.equalsIgnoreCase(this.writeMode)) {
                AdsUtil.checkNecessaryConfig(this.originalConfig, this.writeMode);
                loadModeInit();
            } else if(Constant.INSERTMODE.equalsIgnoreCase(this.writeMode) || Constant.STREAMMODE.equalsIgnoreCase(this.writeMode)) {
                AdsUtil.checkNecessaryConfig(this.originalConfig, this.writeMode);
                List<String> allColumns = AdsInsertUtil.getAdsTableColumnNames(originalConfig);
                AdsInsertUtil.dealColumnConf(originalConfig, allColumns);

                LOG.debug("After job init(), originalConfig now is:[\n{}\n]",
                        originalConfig.toJSON());
            } else {
                throw DataXException.asDataXException(AdsWriterErrorCode.INVALID_CONFIG_VALUE, "writeMode 必须为 'load' 或者 'insert' 或者 'stream'");
            }
        }

        private void loadModeInit() {
            this.adsHelper = AdsUtil.createAdsHelper(this.originalConfig);
            this.odpsAdsHelper = AdsUtil.createAdsHelperWithOdpsAccount(this.originalConfig);
            this.transProjConf = TransferProjectConf.create(this.originalConfig);
            // 打印权限申请流程到日志中
            LOG.info(String
                    .format("%s%n%s%n%s",
                            "如果您直接是odps->ads数据同步, 需要做2方面授权:",
                            "[1] ads官方账号至少需要有待同步表的describe和select权限, 因为ads系统需要获取odps待同步表的结构和数据信息",
                            "[2] 您配置的ads数据源访问账号ak, 需要有向指定的ads数据库发起load data的权限, 您可以在ads系统中添加授权"));
            LOG.info(String
                    .format("%s%s%n%s%n%s",
                            "如果您直接是rds(或其它非odps数据源)->ads数据同步, 流程是先将数据装载如odps临时表，再从odps临时表->ads, ",
                            String.format("中转odps项目为%s,中转项目账号为%s, 权限方面:",
                                    this.transProjConf.getProject(),
                                    this.transProjConf.getAccount()),
                            "[1] ads官方账号至少需要有待同步表(这里是odps临时表)的describe和select权限, 因为ads系统需要获取odps待同步表的结构和数据信息，此部分部署时已经完成授权",
                            String.format("[2] 中转odps对应的账号%s, 需要有向指定的ads数据库发起load data的权限, 您可以在ads系统中添加授权", this.transProjConf.getAccount())));

            /**
             * 如果是从odps导入到ads，直接load data然后System.exit()
             */
            if (super.getPeerPluginName().equals(ODPS_READER)) {
                transferFromOdpsAndExit();
            }
            Account odpsAccount;
            odpsAccount = new AliyunAccount(transProjConf.getAccessId(), transProjConf.getAccessKey());

            Odps odps = new Odps(odpsAccount);
            odps.setEndpoint(transProjConf.getOdpsServer());
            odps.setDefaultProject(transProjConf.getProject());

            TableMeta tableMeta;
            try {
                String adsTable = this.originalConfig.getString(Key.ADS_TABLE);
                TableInfo tableInfo = adsHelper.getTableInfo(adsTable);
                int lifeCycle = this.originalConfig.getInt(Key.Life_CYCLE);
                tableMeta = TableMetaHelper.createTempODPSTable(tableInfo, lifeCycle);
                this.odpsTransTableName = tableMeta.getTableName();
                String sql = tableMeta.toDDL();
                LOG.info("正在创建ODPS临时表： "+sql);
                Instance instance = SQLTask.run(odps, transProjConf.getProject(), sql, null, null);
                boolean terminated = false;
                int time = 0;
                while (!terminated && time < ODPSOVERTIME) {
                    Thread.sleep(1000);
                    terminated = instance.isTerminated();
                    time += 1000;
                }
                LOG.info("正在创建ODPS临时表成功");
            } catch (AdsException e) {
                throw DataXException.asDataXException(AdsWriterErrorCode.ODPS_CREATETABLE_FAILED, e);
            }catch (OdpsException e) {
                throw DataXException.asDataXException(AdsWriterErrorCode.ODPS_CREATETABLE_FAILED,e);
            } catch (InterruptedException e) {
                throw DataXException.asDataXException(AdsWriterErrorCode.ODPS_CREATETABLE_FAILED,e);
            }

            Configuration newConf = AdsUtil.generateConf(this.originalConfig, this.odpsTransTableName,
                    tableMeta, this.transProjConf);
            odpsWriterJobProxy.setPluginJobConf(newConf);
            odpsWriterJobProxy.init();
        }

        /**
         * 当reader是odps的时候，直接call ads的load接口，完成后退出。
         * 这种情况下，用户在odps reader里头填写的参数只有部分有效。
         * 其中accessId、accessKey是忽略掉iao的。
         */
        private void transferFromOdpsAndExit() {
            this.readerConfig = super.getPeerPluginJobConf();
            String odpsTableName = this.readerConfig.getString(Key.ODPSTABLENAME);
            List<String> userConfiguredPartitions = this.readerConfig.getList(Key.PARTITION, String.class);

            if (userConfiguredPartitions == null) {
                userConfiguredPartitions = Collections.emptyList();
            }

            if(userConfiguredPartitions.size() > 1) {
                throw DataXException.asDataXException(AdsWriterErrorCode.ODPS_PARTITION_FAILED, "");
            }

            if(userConfiguredPartitions.size() == 0) {
                loadAdsData(adsHelper, odpsTableName,null);
            }else {
                loadAdsData(adsHelper, odpsTableName,userConfiguredPartitions.get(0));
            }
            System.exit(0);
        }

        // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
        @Override
        public void prepare() {
            if(Constant.LOADMODE.equalsIgnoreCase(this.writeMode)) {
                //导数据到odps表中
                this.odpsWriterJobProxy.prepare();
            } else {
                // 实时表模式非分库分表
                String adsTable = this.originalConfig.getString(Key.ADS_TABLE);
                List<String> preSqls = this.originalConfig.getList(Key.PRE_SQL,
                        String.class);
                List<String> renderedPreSqls = WriterUtil.renderPreOrPostSqls(
                        preSqls, adsTable);
                if (null != renderedPreSqls && !renderedPreSqls.isEmpty()) {
                    // 说明有 preSql 配置，则此处删除掉
                    this.originalConfig.remove(Key.PRE_SQL);
                    Connection preConn = AdsUtil.getAdsConnect(this.originalConfig);
                    LOG.info("Begin to execute preSqls:[{}]. context info:{}.",
                            StringUtils.join(renderedPreSqls, ";"),
                            this.originalConfig.getString(Key.ADS_URL));
                    WriterUtil.executeSqls(preConn, renderedPreSqls,
                            this.originalConfig.getString(Key.ADS_URL),
                            DataBaseType.ADS);
                    DBUtil.closeDBResources(null, null, preConn);
                }
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            if(Constant.LOADMODE.equalsIgnoreCase(this.writeMode)) {
                return this.odpsWriterJobProxy.split(mandatoryNumber);
            } else {
                List<Configuration> splitResult = new ArrayList<Configuration>();
                for(int i = 0; i < mandatoryNumber; i++) {
                    splitResult.add(this.originalConfig.clone());
                }
                return splitResult;
            }
        }

        // 一般来说，是需要推迟到 task 中进行post 的执行（单表情况例外）
        @Override
        public void post() {
            if(Constant.LOADMODE.equalsIgnoreCase(this.writeMode)) {
                loadAdsData(odpsAdsHelper, this.odpsTransTableName, null);
                this.odpsWriterJobProxy.post();
            } else {
                // 实时表模式非分库分表
                String adsTable = this.originalConfig.getString(Key.ADS_TABLE);
                List<String> postSqls = this.originalConfig.getList(
                        Key.POST_SQL, String.class);
                List<String> renderedPostSqls = WriterUtil.renderPreOrPostSqls(
                        postSqls, adsTable);
                if (null != renderedPostSqls && !renderedPostSqls.isEmpty()) {
                    // 说明有 preSql 配置，则此处删除掉
                    this.originalConfig.remove(Key.POST_SQL);
                    Connection postConn = AdsUtil.getAdsConnect(this.originalConfig);
                    LOG.info(
                            "Begin to execute postSqls:[{}]. context info:{}.",
                            StringUtils.join(renderedPostSqls, ";"),
                            this.originalConfig.getString(Key.ADS_URL));
                    WriterUtil.executeSqls(postConn, renderedPostSqls,
                            this.originalConfig.getString(Key.ADS_URL),
                            DataBaseType.ADS);
                    DBUtil.closeDBResources(null, null, postConn);
                }
            }
        }

        @Override
        public void destroy() {
            if(Constant.LOADMODE.equalsIgnoreCase(this.writeMode)) {
                this.odpsWriterJobProxy.destroy();
            } else {
                //insert mode do noting
            }
        }

        private void loadAdsData(AdsHelper helper, String odpsTableName, String odpsPartition) {

            String table = this.originalConfig.getString(Key.ADS_TABLE);
            String project;
            if (super.getPeerPluginName().equals(ODPS_READER)) {
                project = this.readerConfig.getString(Key.PROJECT);
            } else {
                project = this.transProjConf.getProject();
            }
            String partition = this.originalConfig.getString(Key.PARTITION);
            String sourcePath = AdsUtil.generateSourcePath(project,odpsTableName,odpsPartition);
            /**
             * 因为之前检查过，所以不用担心unbox的时候NPE
             */
            boolean overwrite = this.originalConfig.getBool(Key.OVER_WRITE);
            try {
                String id = helper.loadData(table,partition,sourcePath,overwrite);
                LOG.info("ADS Load Data任务已经提交，job id: " + id);
                boolean terminated = false;
                int time = 0;
                while(!terminated) {
                    Thread.sleep(120000);
                    terminated = helper.checkLoadDataJobStatus(id);
                    time += 2;
                    LOG.info("ADS 正在导数据中，整个过程需要20分钟以上，请耐心等待,目前已执行 "+ time+" 分钟");
                }
                LOG.info("ADS 导数据已成功");
            } catch (AdsException e) {
                if (super.getPeerPluginName().equals(ODPS_READER)) {
                    // TODO 使用云账号
                    AdsWriterErrorCode.ADS_LOAD_ODPS_FAILED.setAdsAccount(helper.getUserName());
                    throw DataXException.asDataXException(AdsWriterErrorCode.ADS_LOAD_ODPS_FAILED,e);
                } else {
                    throw DataXException.asDataXException(AdsWriterErrorCode.ADS_LOAD_TEMP_ODPS_FAILED,e);
                }
            } catch (InterruptedException e) {
                throw DataXException.asDataXException(AdsWriterErrorCode.ODPS_CREATETABLE_FAILED,e);
            }
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Writer.Task.class);
        private Configuration writerSliceConfig;
        private OdpsWriter.Task odpsWriterTaskProxy = new OdpsWriter.Task();


        private String writeMode;
        private String schema;
        private String table;
        private int columnNumber;
        // warn: 只有在insert, stream模式才有, 对于load模式表明为odps临时表了
        private TableInfo tableInfo;

        private String writeProxy;
        AdsProxy proxy = null;

        @Override
        public void init() {
            writerSliceConfig = super.getPluginJobConf();
            this.writeMode = this.writerSliceConfig.getString(Key.WRITE_MODE);
            this.schema = writerSliceConfig.getString(Key.SCHEMA);
            this.table =  writerSliceConfig.getString(Key.ADS_TABLE);

            if(Constant.LOADMODE.equalsIgnoreCase(this.writeMode)) {
                odpsWriterTaskProxy.setPluginJobConf(writerSliceConfig);
                odpsWriterTaskProxy.init();
            } else if(Constant.INSERTMODE.equalsIgnoreCase(this.writeMode) || Constant.STREAMMODE.equalsIgnoreCase(this.writeMode)) {

                if (Constant.STREAMMODE.equalsIgnoreCase(this.writeMode)) {
                    this.writeProxy = "datax";
                } else {
                    this.writeProxy = this.writerSliceConfig.getString("writeProxy", "adbClient");
                }
                this.writerSliceConfig.set("writeProxy", this.writeProxy);

                try {
                    this.tableInfo = AdsUtil.createAdsHelper(this.writerSliceConfig).getTableInfo(this.table);
                } catch (AdsException e) {
                    throw DataXException.asDataXException(AdsWriterErrorCode.CREATE_ADS_HELPER_FAILED, e);
                }
                List<String> allColumns = new ArrayList<String>();
                List<ColumnInfo> columnInfo =  this.tableInfo.getColumns();
                for (ColumnInfo eachColumn : columnInfo) {
                    allColumns.add(eachColumn.getName());
                }
                LOG.info("table:[{}] all columns:[\n{}\n].", this.writerSliceConfig.get(Key.ADS_TABLE), StringUtils.join(allColumns, ","));
                AdsInsertUtil.dealColumnConf(writerSliceConfig, allColumns);
                List<String> userColumns = writerSliceConfig.getList(Key.COLUMN, String.class);
                this.columnNumber = userColumns.size();
            } else {
                throw DataXException.asDataXException(AdsWriterErrorCode.INVALID_CONFIG_VALUE, "writeMode 必须为 'load' 或者 'insert' 或者 'stream'");
            }
        }

        @Override
        public void prepare() {
            if(Constant.LOADMODE.equalsIgnoreCase(this.writeMode)) {
                odpsWriterTaskProxy.prepare();
            } else {
                //do nothing
            }
        }

        public void startWrite(RecordReceiver recordReceiver) {
            // 这里的是非odps数据源->odps中转临时表数据同步, load操作在job post阶段完成
            if(Constant.LOADMODE.equalsIgnoreCase(this.writeMode)) {
                odpsWriterTaskProxy.setTaskPluginCollector(super.getTaskPluginCollector());
                odpsWriterTaskProxy.startWrite(recordReceiver);
            } else {
                // insert 模式
                List<String> columns = writerSliceConfig.getList(Key.COLUMN, String.class);
                Connection connection = AdsUtil.getAdsConnect(this.writerSliceConfig);
                TaskPluginCollector taskPluginCollector = super.getTaskPluginCollector();

                if (StringUtils.equalsIgnoreCase(this.writeProxy, "adbClient")) {
                    this.proxy = new AdsClientProxy(table, columns, writerSliceConfig, taskPluginCollector, this.tableInfo);
                } else {
                    this.proxy = new AdsInsertProxy(schema + "." + table, columns, writerSliceConfig, taskPluginCollector, this.tableInfo);
                }
                proxy.startWriteWithConnection(recordReceiver, connection, columnNumber);
            }
        }

        @Override
        public void post() {
            if(Constant.LOADMODE.equalsIgnoreCase(this.writeMode)) {
                odpsWriterTaskProxy.post();
            } else {
                //do noting until now
            }
        }

        @Override
        public void destroy() {
            if(Constant.LOADMODE.equalsIgnoreCase(this.writeMode)) {
                odpsWriterTaskProxy.destroy();
            } else {
                //do noting until now
                if (null != this.proxy) {
                    this.proxy.closeResource();
                }
            }
        }
    }

}
