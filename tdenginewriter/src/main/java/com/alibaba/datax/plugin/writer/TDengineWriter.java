package com.alibaba.datax.plugin.writer;


import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class TDengineWriter extends Writer {

    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String DBNAME = "dbname";
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String PEER_PLUGIN_NAME = "peerPluginName";

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


            String host = this.writerSliceConfig.getString(HOST);
            int port = this.writerSliceConfig.getInt(PORT);
            String dbname = this.writerSliceConfig.getString(DBNAME);
            String user = this.writerSliceConfig.getString(USER);
            String password = this.writerSliceConfig.getString(PASSWORD);

            Properties properties = new Properties();
            String cfgdir = this.writerSliceConfig.getString(JniConnection.PROPERTY_KEY_CONFIG_DIR);
            if (cfgdir != null && !cfgdir.isEmpty()) {
                properties.setProperty(JniConnection.PROPERTY_KEY_CONFIG_DIR, cfgdir);
            }
            String timezone = this.writerSliceConfig.getString(JniConnection.PROPERTY_KEY_TIME_ZONE);
            if (timezone != null && !timezone.isEmpty()) {
                properties.setProperty(JniConnection.PROPERTY_KEY_TIME_ZONE, timezone);
            }
            String locale = this.writerSliceConfig.getString(JniConnection.PROPERTY_KEY_LOCALE);
            if (locale != null && !locale.isEmpty()) {
                properties.setProperty(JniConnection.PROPERTY_KEY_LOCALE, locale);
            }
            String charset = this.writerSliceConfig.getString(JniConnection.PROPERTY_KEY_CHARSET);
            if (charset != null && !charset.isEmpty()) {
                properties.setProperty(JniConnection.PROPERTY_KEY_CHARSET, charset);
            }

            String peerPluginName = this.writerSliceConfig.getString(PEER_PLUGIN_NAME);
            if (peerPluginName.equals("opentsdbreader")) {
                try {
                    JniConnection conn = new JniConnection(properties);
                    conn.open(host, port, dbname, user, password);
                    LOG.info("TDengine connection established, host: " + host + ", port: " + port + ", dbname: " + dbname + ", user: " + user);
                    writeOpentsdb(lineReceiver, conn);
                    conn.close();
                    LOG.info("TDengine connection closed");
                } catch (Exception e) {
                    LOG.error(e.getMessage());
                    e.printStackTrace();
                }
            }
        }

        private void writeOpentsdb(RecordReceiver lineReceiver, JniConnection conn) {
            try {
                Record record;
                while ((record = lineReceiver.getFromReader()) != null) {
                    String jsonData = recordToString(record);
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
