package com.alibaba.datax.plugin.reader.starrocksreader;

import java.util.List;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StarRocksReader extends Reader {

    private static final DataBaseType DATABASE_TYPE = DataBaseType.StarRocks;

    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private Configuration originalConfig = null;
        private CommonRdbmsReader.Job commonRdbmsReaderJob;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            int fetchSize = this.originalConfig.getInt(Constant.FETCH_SIZE,
                    Integer.MIN_VALUE);
            this.originalConfig.set(Constant.FETCH_SIZE, fetchSize);

            this.commonRdbmsReaderJob = new CommonRdbmsReader.Job(DATABASE_TYPE);
            this.commonRdbmsReaderJob.init(this.originalConfig);
        }

        @Override
        public void preCheck(){
            init();
            this.commonRdbmsReaderJob.preCheck(this.originalConfig,DATABASE_TYPE);

        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            LOG.info("split() begin...");
            List<Configuration> splitResult = this.commonRdbmsReaderJob.split(this.originalConfig, adviceNumber);
            /**
             * 在日志中告知用户,为什么实际datax切分跑的channel数会小于用户配置的channel数
             */
            if(splitResult.size() < adviceNumber){
                // 如果用户没有配置切分主键splitPk
                if(StringUtils.isBlank(this.originalConfig.getString(Key.SPLIT_PK, null))){
                    LOG.info("User has not configured splitPk.");
                }else{
                    // 用户配置了切分主键,但是切分主键可能重复太多,或者要同步的表的记录太少,无法切分成adviceNumber个task
                    LOG.info("User has configured splitPk. But the number of task finally split is smaller than that user has configured. " +
                            "The possible reasons are: 1) too many repeated splitPk values, 2) too few records.");
                }
            }
            LOG.info("split() ok and end...");
            return splitResult;
        }

        @Override
        public void post() {
            this.commonRdbmsReaderJob.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderJob.destroy(this.originalConfig);
        }

    }

    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;
        private CommonRdbmsReader.Task commonRdbmsReaderTask;

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsReaderTask = new CommonRdbmsReader.Task(DATABASE_TYPE, super.getTaskGroupId(), super.getTaskId());
            this.commonRdbmsReaderTask.init(this.readerSliceConfig);

        }

        @Override
        public void startRead(RecordSender recordSender) {
            int fetchSize = this.readerSliceConfig.getInt(Constant.FETCH_SIZE);

            this.commonRdbmsReaderTask.startRead(this.readerSliceConfig, recordSender,
                    super.getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void post() {
            this.commonRdbmsReaderTask.post(this.readerSliceConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderTask.destroy(this.readerSliceConfig);
        }

    }
}
