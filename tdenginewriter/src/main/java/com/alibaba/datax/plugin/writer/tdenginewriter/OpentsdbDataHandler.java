package com.alibaba.datax.plugin.writer.tdenginewriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class OpentsdbDataHandler implements DataHandler {
    private static final Logger LOG = LoggerFactory.getLogger(OpentsdbDataHandler.class);
    private static final String DEFAULT_BATCH_SIZE = "1";

    @Override
    public long handle(RecordReceiver lineReceiver, Properties properties, TaskPluginCollector collector) {
        // opentsdb json protocol use JNI and schemaless API to write
        String host = properties.getProperty(Key.HOST);
        int port = Integer.parseInt(properties.getProperty(Key.PORT));
        String dbname = properties.getProperty(Key.DBNAME);
        String user = properties.getProperty(Key.USER);
        String password = properties.getProperty(Key.PASSWORD);

        JniConnection conn = null;
        long count = 0;
        try {
            conn = new JniConnection(properties);
            conn.open(host, port, dbname, user, password);
            LOG.info("TDengine connection established, host: " + host + ", port: " + port + ", dbname: " + dbname + ", user: " + user);
            int batchSize = Integer.parseInt(properties.getProperty(Key.BATCH_SIZE, DEFAULT_BATCH_SIZE));
            count = writeOpentsdb(lineReceiver, conn, batchSize);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (conn != null)
                    conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            LOG.info("TDengine connection closed");
        }

        return count;
    }

    private long writeOpentsdb(RecordReceiver lineReceiver, JniConnection conn, int batchSize) {
        long recordIndex = 1;
        try {
            Record record;
            StringBuilder sb = new StringBuilder();
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
        return recordIndex - 1;
    }

    private String recordToString(Record record) {
        int recordLength = record.getColumnNumber();
        if (0 == recordLength) {
            return "";
        }
        Column column;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < recordLength; i++) {
            column = record.getColumn(i);
            sb.append(column.asString()).append("\t");
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }
}
