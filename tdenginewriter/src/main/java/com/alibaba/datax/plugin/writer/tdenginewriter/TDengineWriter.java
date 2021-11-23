package com.alibaba.datax.plugin.writer.tdenginewriter;


import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class TDengineWriter extends Writer {

    private static final String PEER_PLUGIN_NAME = "peerPluginName";

    static {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static class Job extends Writer.Job {

        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.originalConfig.set(PEER_PLUGIN_NAME, getPeerPluginName());
        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> writerSplitConfigs = new ArrayList<>();
            for (int i = 0; i < mandatoryNumber; i++) {
                writerSplitConfigs.add(this.originalConfig);
            }
            return writerSplitConfigs;
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration writerSliceConfig;

        @Override
        public void init() {
            this.writerSliceConfig = getPluginJobConf();

        }

        @Override
        public void destroy() {

        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            Set<String> keys = this.writerSliceConfig.getKeys();
            Properties properties = new Properties();
            for (String key : keys) {
                String value = this.writerSliceConfig.getString(key);
                properties.setProperty(key, value);
            }
            if (!keys.contains(Key.USER)) {
                properties.setProperty(Key.USER, "root");
            }
            if (!keys.contains(Key.PASSWORD)) {
                properties.setProperty(Key.PASSWORD, "taosdata");
            }
            LOG.debug("========================properties==========================\n" + properties);
            String peerPluginName = this.writerSliceConfig.getString(PEER_PLUGIN_NAME);
            LOG.debug("start to handle record from: " + peerPluginName);
            DataHandler handler = DataHandlerFactory.build(peerPluginName);
            long records = handler.handle(lineReceiver, properties, getTaskPluginCollector());
            LOG.debug("handle data finished, records: " + records);
        }

    }
}
