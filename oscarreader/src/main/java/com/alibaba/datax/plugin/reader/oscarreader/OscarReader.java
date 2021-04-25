package com.alibaba.datax.plugin.reader.oscarreader;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;

import java.util.List;

/**
 * 神通数据库读取插件
 * @author LockiJiang
 * @date 2021/4/23
 */
public class OscarReader extends Reader {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.Oscar;

    public static class Job extends Reader.Job{
        private Configuration originalConfig = null;
        private CommonRdbmsReader.Job commonRdbmsReaderJob;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            int fetchSize = this.originalConfig.getInt(Constant.FETCH_SIZE, com.alibaba.datax.plugin.reader.oscarreader.Constant.DEFAULT_FETCH_SIZE);
            this.originalConfig.set(Constant.FETCH_SIZE, fetchSize);
            this.commonRdbmsReaderJob = new CommonRdbmsReader.Job(DATABASE_TYPE);
            this.commonRdbmsReaderJob.init(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return this.commonRdbmsReaderJob.split(this.originalConfig, adviceNumber);
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

    public static class Task extends Reader.Task{
        private Configuration readerSliceConfig;
        private CommonRdbmsReader.Task commonRdbmsReaderTask;

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsReaderTask = new CommonRdbmsReader.Task(DATABASE_TYPE , super.getTaskGroupId(), super.getTaskId());
            this.commonRdbmsReaderTask.init(this.readerSliceConfig);
        }

        @Override
        public void startRead(RecordSender recordSender) {
            int fetchSize = this.readerSliceConfig.getInt(com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE);
            this.commonRdbmsReaderTask.startRead(this.readerSliceConfig, recordSender, super.getTaskPluginCollector(), fetchSize);
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
