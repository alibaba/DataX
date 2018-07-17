package com.alibaba.datax.plugin.writer.hbase11xsqlwriter;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yanghan.y
 */
public class HbaseSQLWriter extends Writer {
    public static class Job extends Writer.Job {
        private HbaseSQLWriterConfig config;

        @Override
        public void init() {
            // 解析配置
            config = HbaseSQLHelper.parseConfig(this.getPluginJobConf());

            // 校验配置，会访问集群来检查表
            HbaseSQLHelper.validateConfig(config);
        }

        @Override
        public void  prepare() {
            // 写之前是否要清空目标表，默认不清空
            if(config.truncate()) {
                Connection conn = HbaseSQLHelper.getJdbcConnection(config);
                HbaseSQLHelper.truncateTable(conn, config.getTableName());
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> splitResultConfigs = new ArrayList<Configuration>();
            for (int j = 0; j < mandatoryNumber; j++) {
                splitResultConfigs.add(config.getOriginalConfig().clone());
            }
            return splitResultConfigs;
        }

        @Override
        public void destroy() {
            // NOOP
        }
    }

    public static class Task extends Writer.Task {
        private Configuration taskConfig;
        private HbaseSQLWriterTask hbaseSQLWriterTask;

        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();
            this.hbaseSQLWriterTask = new HbaseSQLWriterTask(this.taskConfig);
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            this.hbaseSQLWriterTask.startWriter(lineReceiver, super.getTaskPluginCollector());
        }


        @Override
        public void destroy() {
            // hbaseSQLTask不需要close
        }
    }
}
