package com.alibaba.datax.plugin.writer;


import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TDengineWriter extends Writer {

    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String DBNAME = "dbname";
    private static final String USER = "user";
    private static final String PASSWORD = "password";

    public static class Job extends Writer.Job {

        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
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
        private static final String NEWLINE_FLAG = System.getProperty("line.separator", "\n");
        private Configuration writerSliceConfig;
        private String peerPluginName;

        @Override
        public void init() {
            this.writerSliceConfig = getPluginJobConf();
            this.peerPluginName = getPeerPluginName();
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

            JniConnection connection = new JniConnection(new Properties());
            long psql = connection.open(host, port, dbname, user, password);
            System.out.println("psql: " + psql);
            connection.close();

            try {
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));

                Record record;
                while ((record = lineReceiver.getFromReader()) != null) {
                    writer.write(recordToString(record));
                }
                writer.flush();

            } catch (Exception e) {
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
