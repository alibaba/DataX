package com.alibaba.datax.plugin.reader.iotdbreader;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.iotdbwriter.IoTDBWriter;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.isession.util.Version;
import org.junit.Assume;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IoTDBPluginRemoteTest {
    private static final double EXPECTED_VALUE = 42.5d;

    @Test
    public void writerAndReaderTasksRoundTripTreeRecord() throws Exception {
        String endpoint = System.getenv("IOTDB_TEST_ENDPOINT");
        Assume.assumeTrue("Set IOTDB_TEST_ENDPOINT=host:port to run the remote smoke test.",
                endpoint != null && endpoint.contains(":"));

        String[] endpointParts = endpoint.split(":", 2);
        String host = endpointParts[0];
        int port = Integer.parseInt(endpointParts[1]);
        String username = environmentOrDefault("IOTDB_TEST_USERNAME", "root");
        String password = environmentOrDefault("IOTDB_TEST_PASSWORD", "root");
        String database = "root.datax_smoke_" + UUID.randomUUID().toString().replace("-", "");
        String device = database + ".sensor";
        long timestamp = System.currentTimeMillis();
        Exception failure = null;

        try {
            runWriter(host, port, username, password, database, device, timestamp);
            List<Record> records = runReader(host, port, username, password, device, timestamp);

            assertEquals(1, records.size());
            assertEquals(Long.valueOf(timestamp), records.get(0).getColumn(0).asLong());
            assertEquals(EXPECTED_VALUE, records.get(0).getColumn(1).asDouble(), 0.00001d);
        } catch (Exception e) {
            failure = e;
            throw e;
        } finally {
            try {
                dropDatabase(host, port, username, password, database);
            } catch (Exception cleanupFailure) {
                if (failure != null) {
                    failure.addSuppressed(cleanupFailure);
                } else {
                    throw cleanupFailure;
                }
            }
        }
    }

    private static void runWriter(String host, int port, String username, String password,
                                  String database, String device, long timestamp) {
        Configuration configuration = Configuration.from("{"
                + "\"model\":\"tree\",\"host\":\"" + host + "\",\"port\":" + port + ","
                + "\"username\":\"" + username + "\",\"password\":\"" + password + "\","
                + "\"device\":\"" + device + "\",\"measurements\":[\"value\"],"
                + "\"dataTypes\":[\"DOUBLE\"],\"batchSize\":10,"
                + "\"preSql\":[\"CREATE DATABASE " + database + "\"]}");
        IoTDBWriter.Task task = new IoTDBWriter.Task();
        task.setPluginJobConf(configuration);
        task.setTaskPluginCollector(new FailingCollector());
        try {
            task.init();
            task.prepare();
            task.startWrite(new SingleRecordReceiver(record(
                    new DateColumn(timestamp), new DoubleColumn(EXPECTED_VALUE))));
        } catch (DataXException e) {
            throw e;
        } finally {
            task.destroy();
        }
    }

    private static List<Record> runReader(String host, int port, String username, String password,
                                          String device, long timestamp) {
        Configuration configuration = Configuration.from("{"
                + "\"model\":\"tree\",\"host\":\"" + host + "\",\"port\":" + port + ","
                + "\"username\":\"" + username + "\",\"password\":\"" + password + "\","
                + "\"device\":\"" + device + "\",\"measurements\":[\"value\"],"
                + "\"where\":\"time = " + timestamp + "\"}");
        IoTDBReader.Task task = new IoTDBReader.Task();
        task.setPluginJobConf(configuration);
        task.setTaskPluginCollector(new FailingCollector());
        CapturingSender sender = new CapturingSender();
        try {
            task.init();
            task.startRead(sender);
            return sender.records;
        } finally {
            task.destroy();
        }
    }

    private static void dropDatabase(String host, int port, String username, String password,
                                     String database)
            throws IoTDBConnectionException, StatementExecutionException {
        Session session = new Session.Builder()
                .host(host)
                .port(port)
                .username(username)
                .password(password)
                .version(Version.V_1_0)
                .build();
        try {
            session.open();
            session.executeNonQueryStatement("DROP DATABASE " + database);
        } finally {
            session.close();
        }
    }

    private static String environmentOrDefault(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.trim().isEmpty() ? fallback : value;
    }

    private static Record record(Column... columns) {
        TestRecord record = new TestRecord();
        for (Column column : columns) {
            record.addColumn(column);
        }
        return record;
    }

    private static final class SingleRecordReceiver implements RecordReceiver {
        private Record record;

        private SingleRecordReceiver(Record record) {
            this.record = record;
        }

        @Override
        public Record getFromReader() {
            Record next = record;
            record = null;
            return next;
        }

        @Override
        public void shutdown() {
        }
    }

    private static final class CapturingSender implements RecordSender {
        private final List<Record> records = new ArrayList<>();

        @Override
        public Record createRecord() {
            return new TestRecord();
        }

        @Override
        public void sendToWriter(Record record) {
            records.add(record);
        }

        @Override
        public void flush() {
        }

        @Override
        public void terminate() {
        }

        @Override
        public void shutdown() {
        }
    }

    private static final class FailingCollector extends TaskPluginCollector {
        @Override
        public void collectDirtyRecord(Record record, Throwable error, String message) {
            throw new AssertionError(message, error);
        }

        @Override
        public void collectMessage(String key, String value) {
        }
    }

    private static final class TestRecord implements Record {
        private final List<Column> columns = new ArrayList<>();

        @Override
        public void addColumn(Column column) {
            columns.add(column);
        }

        @Override
        public void setColumn(int index, Column column) {
            columns.set(index, column);
        }

        @Override
        public Column getColumn(int index) {
            return columns.get(index);
        }

        @Override
        public int getColumnNumber() {
            return columns.size();
        }

        @Override
        public int getByteSize() {
            return 0;
        }

        @Override
        public int getMemorySize() {
            return 0;
        }

        @Override
        public void setMeta(java.util.Map<String, String> meta) {
        }

        @Override
        public java.util.Map<String, String> getMeta() {
            return null;
        }
    }
}
