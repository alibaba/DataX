package com.alibaba.datax.plugin.reader.hbase11xsqlreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HbaseSQLReader extends Reader {
    public static class Job extends Reader.Job {
        private HbaseSQLReaderConfig readerConfig;

        @Override
        public void init() {
            readerConfig = HbaseSQLHelper.parseConfig(this.getPluginJobConf());
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return HbaseSQLHelper.split(readerConfig);
        }


        @Override
        public void destroy() {

        }

    }

    public static class Task extends Reader.Task {
        private static Logger LOG = LoggerFactory.getLogger(Task.class);
        private HbaseSQLReaderTask hbase11SQLReaderTask;

        @Override
        public void init() {
            hbase11SQLReaderTask = new HbaseSQLReaderTask(this.getPluginJobConf());
            this.hbase11SQLReaderTask.init();
        }

        @Override
        public void prepare() {
            hbase11SQLReaderTask.prepare();
        }


        @Override
        public void startRead(RecordSender recordSender) {
            Long recordNum = 0L;
            Record record = recordSender.createRecord();
            boolean fetchOK;
            while (true) {
                try {
                    fetchOK =  this.hbase11SQLReaderTask.readRecord(record);
                } catch (Exception e) {
                    LOG.info("Read record exception", e);
                    e.printStackTrace();
                    super.getTaskPluginCollector().collectDirtyRecord(record, e);
                    record = recordSender.createRecord();
                    continue;
                }
                if (fetchOK) {
                    recordSender.sendToWriter(record);
                    recordNum++;
                    if (recordNum % 10000 == 0)
                        LOG.info("already read record num is " + recordNum);
                    record = recordSender.createRecord();
                } else {
                    break;
                }
            }
            recordSender.flush();
        }

        @Override
        public void post() {
            super.post();
        }

        @Override
        public void destroy() {
            this.hbase11SQLReaderTask.destroy();
        }
    }

}
