package com.alibaba.datax.plugin.writer.hbase20xsqlwriter;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;

import java.util.ArrayList;
import java.util.List;

public class HBase20xSQLWriter extends Writer {

    public static class Job extends Writer.Job {

        private Configuration config = null;

        @Override
        public void init() {
            this.config = this.getPluginJobConf();
            HBase20xSQLHelper.validateParameter(this.config);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> splitResultConfigs = new ArrayList<Configuration>();
            for (int j = 0; j < mandatoryNumber; j++) {
                splitResultConfigs.add(config.clone());
            }
            return splitResultConfigs;
        }

        @Override
        public void destroy() {
            //doNothing
        }
    }

    public static class Task extends Writer.Task {
        private Configuration taskConfig;
        private HBase20xSQLWriterTask writerTask;

        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();
            this.writerTask = new HBase20xSQLWriterTask(this.taskConfig);
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            this.writerTask.startWriter(lineReceiver, super.getTaskPluginCollector());
        }


        @Override
        public void destroy() {
            // 不需要close
        }
    }
}