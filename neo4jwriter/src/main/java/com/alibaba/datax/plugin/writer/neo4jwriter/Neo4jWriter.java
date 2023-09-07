package com.alibaba.datax.plugin.writer.neo4jwriter;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.element.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Neo4jWriter extends Writer {
    public static class Job extends Writer.Job {
        private static final Logger LOGGER = LoggerFactory.getLogger(Job.class);

        private Configuration jobConf = null;
        @Override
        public void init() {
            LOGGER.info("Neo4jWriter Job init success");
            this.jobConf = getPluginJobConf();
        }

        @Override
        public void destroy() {
            LOGGER.info("Neo4jWriter Job destroyed");
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configurations = new ArrayList<Configuration>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(this.jobConf.clone());
            }
            return configurations;
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger TASK_LOGGER = LoggerFactory.getLogger(Task.class);
        private Neo4jClient neo4jClient;
        @Override
        public void init() {
            Configuration taskConf = super.getPluginJobConf();
            this.neo4jClient = Neo4jClient.build(taskConf,getTaskPluginCollector());
            this.neo4jClient.init();
            TASK_LOGGER.info("neo4j writer task init success.");
        }

        @Override
        public void destroy() {
            this.neo4jClient.destroy();
            TASK_LOGGER.info("neo4j writer task destroyed.");
        }

        @Override
        public void startWrite(RecordReceiver receiver) {
            Record record;
            while ((record = receiver.getFromReader()) != null){
                this.neo4jClient.tryWrite(record);
            }
        }
    }
}
