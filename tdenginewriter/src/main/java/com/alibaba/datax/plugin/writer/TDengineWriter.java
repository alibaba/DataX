package com.alibaba.datax.plugin.writer;


import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TDengineWriter extends Writer {

    private static final String PEER_PLUGIN_NAME = "peerPluginName";
    private static final String DEFAULT_BATCH_SIZE = "1";

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

        private static final String NEWLINE_FLAG = System.getProperty("line.separator", "\n");
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

            String peerPluginName = this.writerSliceConfig.getString(PEER_PLUGIN_NAME);
            if (peerPluginName.equals("opentsdbreader")) {
                // opentsdb json protocol use JNI and schemaless API to write

                String host = properties.getProperty(Key.HOST);
                int port = Integer.parseInt(properties.getProperty(Key.PORT));
                String dbname = properties.getProperty(Key.DBNAME);
                String user = properties.getProperty(Key.USER);
                String password = properties.getProperty(Key.PASSWORD);

                try {
                    JniConnection conn = new JniConnection(properties);
                    conn.open(host, port, dbname, user, password);
                    LOG.info("TDengine connection established, host: " + host + ", port: " + port + ", dbname: " + dbname + ", user: " + user);

                    int batchSize = Integer.parseInt(properties.getProperty(Key.BATCH_SIZE, DEFAULT_BATCH_SIZE));
                    writeOpentsdb(lineReceiver, conn, batchSize);
                    conn.close();
                    LOG.info("TDengine connection closed");
                } catch (Exception e) {
                    LOG.error(e.getMessage());
                    e.printStackTrace();
                }
            } else {
                // other
            }
        }

        private void writeOpentsdb(RecordReceiver lineReceiver, JniConnection conn, int batchSize) {
            try {
                Record record;
                StringBuilder sb = new StringBuilder();
                long recordIndex = 1;
                while ((record = lineReceiver.getFromReader()) != null) {
                    if (batchSize == 1) {
                        String jsonData = recordToString(record);
                        LOG.debug(">>> " + jsonData);
                        conn.insertOpentsdbJson(jsonData);
                    } else if (recordIndex % batchSize == 1) {
                        sb.append("[").append(recordToString(record)).append(",");
                    } else if (recordIndex % batchSize == 0) {
                        sb.append(recordToString(record)).append("]");
                        String jsonData = sb.toString();
                        LOG.debug(">>> " + jsonData);
                        conn.insertOpentsdbJson(jsonData);
                        sb.delete(0, sb.length());
                    } else {
                        sb.append(recordToString(record)).append(",");
                    }
                    recordIndex++;
                }
                if (sb.length() != 0 && sb.charAt(0) == '[') {
                    String jsonData = sb.deleteCharAt(sb.length() - 1).append("]").toString();
                    LOG.debug(">>> " + jsonData);
                    conn.insertOpentsdbJson(jsonData);
                }
            } catch (Exception e) {
                LOG.error("TDengineWriter ERROR: " + e.getMessage());
                throw DataXException.asDataXException(TDengineWriterErrorCode.RUNTIME_EXCEPTION, e);
            }
        }

        private String recordToString(Record record) {
            int recordLength = record.getColumnNumber();
            if (0 == recordLength) {
                return NEWLINE_FLAG;
            }
            Column column;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < recordLength; i++) {
                column = record.getColumn(i);
                sb.append(column.asString()).append("\t");
            }
            sb.setLength(sb.length() - 1);
            sb.append(NEWLINE_FLAG);
            return sb.toString();
        }
    }
}
