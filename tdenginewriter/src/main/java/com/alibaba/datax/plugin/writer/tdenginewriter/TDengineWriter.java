package com.alibaba.datax.plugin.writer.tdenginewriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TDengineWriter extends Writer {

    private static final String PEER_PLUGIN_NAME = "peerPluginName";

    public static class Job extends Writer.Job {

        private Configuration originalConfig;
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.originalConfig.set(PEER_PLUGIN_NAME, getPeerPluginName());

            // check username
            String user = this.originalConfig.getString(Key.USERNAME);
            if (StringUtils.isBlank(user))
                throw DataXException.asDataXException(TDengineWriterErrorCode.REQUIRED_VALUE, "The parameter ["
                        + Key.USERNAME + "] is not set.");

            // check password
            String password = this.originalConfig.getString(Key.PASSWORD);
            if (StringUtils.isBlank(password))
                throw DataXException.asDataXException(TDengineWriterErrorCode.REQUIRED_VALUE, "The parameter ["
                        + Key.PASSWORD + "] is not set.");

            // check connection
            List<Object> connection = this.originalConfig.getList(Key.CONNECTION);
            if (connection == null || connection.isEmpty())
                throw DataXException.asDataXException(TDengineWriterErrorCode.REQUIRED_VALUE, "The parameter ["
                        + Key.CONNECTION + "] is not set.");
            if (connection.size() > 1)
                LOG.warn("connection.size is " + connection.size() + " and only connection[0] will be used.");
            Configuration conn = Configuration.from(connection.get(0).toString());
            String jdbcUrl = conn.getString(Key.JDBC_URL);
            if (StringUtils.isBlank(jdbcUrl))
                throw DataXException.asDataXException(TDengineWriterErrorCode.REQUIRED_VALUE, "The parameter ["
                        + Key.JDBC_URL + "] of connection is not set.");

            // check column
        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> writerSplitConfigs = new ArrayList<>();

            List<Object> conns = this.originalConfig.getList(Key.CONNECTION);
            for (int i = 0; i < mandatoryNumber; i++) {
                Configuration clone = this.originalConfig.clone();
                Configuration conf = Configuration.from(conns.get(0).toString());
                String jdbcUrl = conf.getString(Key.JDBC_URL);
                clone.set(Key.JDBC_URL, jdbcUrl);
                clone.set(Key.TABLE, conf.getList(Key.TABLE));
                clone.remove(Key.CONNECTION);
                writerSplitConfigs.add(clone);
            }

            return writerSplitConfigs;
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;
        private TaskPluginCollector taskPluginCollector;

        @Override
        public void init() {
            this.writerSliceConfig = getPluginJobConf();
            this.taskPluginCollector = super.getTaskPluginCollector();
        }

        @Override
        public void destroy() {

        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            String peerPluginName = this.writerSliceConfig.getString(PEER_PLUGIN_NAME);
            LOG.debug("start to handle record from: " + peerPluginName);

            DataHandler handler;
            if (peerPluginName.equals("opentsdbreader"))
                handler = new OpentsdbDataHandler(this.writerSliceConfig);
            else
                handler = new DefaultDataHandler(this.writerSliceConfig, this.taskPluginCollector);

            long records = handler.handle(lineReceiver, getTaskPluginCollector());
            LOG.debug("handle data finished, records: " + records);
        }

    }
}
