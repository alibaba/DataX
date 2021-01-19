package com.alibaba.datax.plugin.reader.kudureader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author daizihao
 * @create 2021-01-15 16:15
 **/
public class KuduReader extends Reader {
    public static class Job extends Reader.Job {
        private Configuration originConfig = null;

        @Override
        public void init() {
            this.originConfig = this.getPluginJobConf();
            KuduReaderHelper.validateParameter(this.originConfig);
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
//            return KuduReaderHelper.split(this.originConfig);
            return null;
        }


        @Override
        public void destroy() {

        }

    }
    public static class Task extends Reader.Task {
        private Configuration taskConfig;
        private static Logger LOG = LoggerFactory.getLogger(Task.class);
        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();


        }

        @Override
        public void prepare() {

        }

        @Override
        public void startRead(RecordSender recordSender) {

        }

        @Override
        public void post() {
            super.post();
        }

        @Override
        public void destroy() {

        }
    }
}
