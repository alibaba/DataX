package com.alibaba.datax.plugin.writer.adbmysqlwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class AdbMysqlWriter extends Writer {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.ADB;

    public static class Job extends Writer.Job {
        private Configuration originalConfig = null;
        private CommonRdbmsWriter.Job commonRdbmsWriterJob;

        @Override
        public void preCheck(){
            this.init();
            this.commonRdbmsWriterJob.writerPreCheck(this.originalConfig, DATABASE_TYPE);
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.commonRdbmsWriterJob = new CommonRdbmsWriter.Job(DATABASE_TYPE);
            this.commonRdbmsWriterJob.init(this.originalConfig);
        }

        // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
        @Override
        public void prepare() {
            //实跑先不支持 权限 检验
            //this.commonRdbmsWriterJob.privilegeValid(this.originalConfig, DATABASE_TYPE);
            this.commonRdbmsWriterJob.prepare(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return this.commonRdbmsWriterJob.split(this.originalConfig, mandatoryNumber);
        }

        // 一般来说，是需要推迟到 task 中进行post 的执行（单表情况例外）
        @Override
        public void post() {
            this.commonRdbmsWriterJob.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterJob.destroy(this.originalConfig);
        }

    }

    public static class Task extends Writer.Task {

        private Configuration writerSliceConfig;
        private CommonRdbmsWriter.Task commonRdbmsWriterTask;

        public static class DelegateClass extends CommonRdbmsWriter.Task {
            private long writeTime = 0L;
            private long writeCount = 0L;
            private long lastLogTime = 0;

            public DelegateClass(DataBaseType dataBaseType) {
                super(dataBaseType);
            }

            @Override
            protected void doBatchInsert(Connection connection, List<Record> buffer)
                    throws SQLException {
                long startTime = System.currentTimeMillis();

                super.doBatchInsert(connection, buffer);

                writeCount = writeCount + buffer.size();
                writeTime = writeTime + (System.currentTimeMillis() - startTime);

                // log write metrics every 10 seconds
                if (System.currentTimeMillis() - lastLogTime > 10000) {
                    lastLogTime = System.currentTimeMillis();
                    logTotalMetrics();
                }
            }

            public void logTotalMetrics() {
                LOG.info(Thread.currentThread().getName() + ", AdbMySQL writer take " + writeTime + " ms, write " + writeCount + " records.");
            }
        }

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();

            if (StringUtils.isBlank(this.writerSliceConfig.getString(Key.WRITE_MODE))) {
                this.writerSliceConfig.set(Key.WRITE_MODE, "REPLACE");
            }

            this.commonRdbmsWriterTask = new DelegateClass(DATABASE_TYPE);
            this.commonRdbmsWriterTask.init(this.writerSliceConfig);
        }

        @Override
        public void prepare() {
            this.commonRdbmsWriterTask.prepare(this.writerSliceConfig);
        }

        //TODO 改用连接池，确保每次获取的连接都是可用的（注意：连接可能需要每次都初始化其 session）
        public void startWrite(RecordReceiver recordReceiver) {
            this.commonRdbmsWriterTask.startWrite(recordReceiver, this.writerSliceConfig,
                    super.getTaskPluginCollector());
        }

        @Override
        public void post() {
            this.commonRdbmsWriterTask.post(this.writerSliceConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterTask.destroy(this.writerSliceConfig);
        }

        @Override
        public boolean supportFailOver(){
            String writeMode = writerSliceConfig.getString(Key.WRITE_MODE);
            return "replace".equalsIgnoreCase(writeMode);
        }

    }
}
