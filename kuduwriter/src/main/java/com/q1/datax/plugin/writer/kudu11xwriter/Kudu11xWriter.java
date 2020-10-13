package com.q1.datax.plugin.writer.kudu11xwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author daizihao
 * @create 2020-08-27 16:58
 **/
public class Kudu11xWriter extends Writer {
    public static class Job extends Writer.Job{
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration config = null;
        @Override
        public void init() {
            this.config = this.getPluginJobConf();
            Kudu11xHelper.validateParameter(this.config);
        }

        @Override
        public void prepare() {
            Boolean truncate = config.getBool(Key.TRUNCATE,false);
            if(truncate){
                Kudu11xHelper.truncateTable(this.config);
            }

            if (!Kudu11xHelper.isTableExists(config)){
                Kudu11xHelper.createTable(config);
            }
        }

        @Override
        public List<Configuration> split(int i) {
            List<Configuration> splitResultConfigs = new ArrayList<>();
            for (int j = 0; j < i; j++) {
                splitResultConfigs.add(config.clone());
            }

            return splitResultConfigs;
        }



        @Override
        public void destroy() {

        }
    }

    public static class Task extends Writer.Task{
        private Configuration taskConfig;
        private KuduWriterTask kuduTaskProxy;
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();
            this.kuduTaskProxy = new KuduWriterTask(this.taskConfig);
        }
        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            this.kuduTaskProxy.startWriter(lineReceiver,super.getTaskPluginCollector());
        }


        @Override
        public void destroy() {
            try {
                if (kuduTaskProxy.session != null) {
                    kuduTaskProxy.session.close();
                }
            }catch (Exception e){
                LOG.warn("The \"kudu session\" was not stopped gracefully !");
            }
            Kudu11xHelper.closeClient(kuduTaskProxy.kuduClient);

        }
    }
}
