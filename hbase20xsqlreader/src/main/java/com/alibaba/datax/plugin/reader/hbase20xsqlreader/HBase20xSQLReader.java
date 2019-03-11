package com.alibaba.datax.plugin.reader.hbase20xsqlreader;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;

import java.util.List;

public class HBase20xSQLReader extends Reader {

    public static class Job extends Reader.Job {
        private Configuration originalConfig;
        private HBase20SQLReaderHelper readerHelper;
        @Override
        public void init() {
            this.originalConfig = this.getPluginJobConf();
            this.readerHelper = new HBase20SQLReaderHelper(this.originalConfig);
            readerHelper.validateParameter();
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return readerHelper.doSplit(adviceNumber);
        }

        @Override
        public void destroy() {
            // do nothing
        }
    }

    public static class Task extends Reader.Task {
        private Configuration readerConfig;
        private HBase20xSQLReaderTask hbase20xSQLReaderTask;

        @Override
        public void init() {
            this.readerConfig = super.getPluginJobConf();
            hbase20xSQLReaderTask = new HBase20xSQLReaderTask(readerConfig, super.getTaskGroupId(), super.getTaskId());
        }

        @Override
        public void startRead(RecordSender recordSender) {
            hbase20xSQLReaderTask.readRecord(recordSender);
        }

        @Override
        public void destroy() {
            // do nothing
        }

    }
}
