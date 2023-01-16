package com.alibaba.datax.plugin.writer.tdenginewriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.taosdata.jdbc.SchemalessWriter;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class OpentsdbDataHandler implements DataHandler {
    private static final Logger LOG = LoggerFactory.getLogger(OpentsdbDataHandler.class);
    private SchemalessWriter writer;

    private String jdbcUrl;
    private String user;
    private String password;
    int batchSize;

    public OpentsdbDataHandler(Configuration config) {
        // opentsdb json protocol use JNI and schemaless API to write
        this.jdbcUrl = config.getString(Key.JDBC_URL);
        this.user = config.getString(Key.USERNAME, "root");
        this.password = config.getString(Key.PASSWORD, "taosdata");
        this.batchSize = config.getInt(Key.BATCH_SIZE, Constants.DEFAULT_BATCH_SIZE);
    }

    @Override
    public int handle(RecordReceiver lineReceiver, TaskPluginCollector collector) {
        int count = 0;
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);) {
            LOG.info("connection[ jdbcUrl: " + jdbcUrl + ", username: " + user + "] established.");
            writer = new SchemalessWriter(conn);
            count = write(lineReceiver, batchSize);
        } catch (Exception e) {
            throw DataXException.asDataXException(TDengineWriterErrorCode.RUNTIME_EXCEPTION, e);
        }

        return count;
    }

    private int write(RecordReceiver lineReceiver, int batchSize) throws DataXException {
        int recordIndex = 1;
        try {
            Record record;
            StringBuilder sb = new StringBuilder();
            while ((record = lineReceiver.getFromReader()) != null) {
                if (batchSize == 1) {
                    String jsonData = recordToString(record);
                    LOG.debug(">>> " + jsonData);
                    writer.write(jsonData, SchemalessProtocolType.JSON, SchemalessTimestampType.NOT_CONFIGURED);
                } else if (recordIndex % batchSize == 1) {
                    sb.append("[").append(recordToString(record)).append(",");
                } else if (recordIndex % batchSize == 0) {
                    sb.append(recordToString(record)).append("]");
                    String jsonData = sb.toString();
                    LOG.debug(">>> " + jsonData);
                    writer.write(jsonData, SchemalessProtocolType.JSON, SchemalessTimestampType.NOT_CONFIGURED);
                    sb.delete(0, sb.length());
                } else {
                    sb.append(recordToString(record)).append(",");
                }
                recordIndex++;
            }
            if (sb.length() != 0 && sb.charAt(0) == '[') {
                String jsonData = sb.deleteCharAt(sb.length() - 1).append("]").toString();
                System.err.println(jsonData);
                LOG.debug(">>> " + jsonData);
                writer.write(jsonData, SchemalessProtocolType.JSON, SchemalessTimestampType.NOT_CONFIGURED);
            }
        } catch (Exception e) {
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
