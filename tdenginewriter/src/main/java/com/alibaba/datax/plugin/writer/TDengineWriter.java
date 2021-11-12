package com.alibaba.datax.plugin.writer;


import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

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
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
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
            String peerPluginName = this.writerSliceConfig.getString(PEER_PLUGIN_NAME);
            LOG.debug("start to handle record from: " + peerPluginName);
            DataHandler handler = DataHandlerFactory.build(peerPluginName);
            long records = handler.handle(lineReceiver, writerSliceConfig);
            LOG.debug("handle data finished, records: " + records);
        }

    }
}
