package com.alibaba.datax.plugin.reader.iotdbreader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.TableSessionBuilder;
import org.apache.tsfile.read.common.RowRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IoTDBReader extends Reader {

    public static class Job extends Reader.Job {
        private Configuration jobConf;

        @Override
        public void init() {
            jobConf = super.getPluginJobConf();
            IoTDBReaderConfig.validateAndSetDefaults(jobConf);
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<String> queries = IoTDBReaderConfig.queries(jobConf);
            int taskCount = Math.min(Math.max(1, adviceNumber), queries.size());
            List<Configuration> result = new ArrayList<>(taskCount);
            for (int i = 0; i < taskCount; i++) {
                List<String> assignedQueries = new ArrayList<>();
                for (int queryIndex = i; queryIndex < queries.size(); queryIndex += taskCount) {
                    assignedQueries.add(queries.get(queryIndex));
                }
                Configuration taskConf = jobConf.clone();
                taskConf.set(Key.QUERY_SQLS, assignedQueries);
                result.add(taskConf);
            }
            return result;
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration taskConf;
        private Session treeSession;
        private ITableSession tableSession;

        @Override
        public void init() {
            taskConf = super.getPluginJobConf();
            IoTDBReaderConfig.validateAndSetDefaults(taskConf);
            try {
                if ("table".equals(taskConf.getString(Key.MODEL))) {
                    TableSessionBuilder builder = new TableSessionBuilder()
                            .nodeUrls(Collections.singletonList(
                                    taskConf.getString(Key.HOST) + ":" + taskConf.getInt(Key.PORT)))
                            .username(taskConf.getString(Key.USERNAME))
                            .password(taskConf.getString(Key.PASSWORD))
                            .fetchSize(taskConf.getInt(Key.FETCH_SIZE))
                            .queryTimeoutInMs(taskConf.getLong(Key.QUERY_TIMEOUT_MS));
                    String database = taskConf.getString(Key.DATABASE);
                    if (database != null && !database.trim().isEmpty()) {
                        builder.database(database.trim());
                    }
                    tableSession = builder.build();
                } else {
                    treeSession = new Session.Builder()
                            .host(taskConf.getString(Key.HOST))
                            .port(taskConf.getInt(Key.PORT))
                            .username(taskConf.getString(Key.USERNAME))
                            .password(taskConf.getString(Key.PASSWORD))
                            .version(Version.valueOf(taskConf.getString(Key.VERSION)))
                            .fetchSize(taskConf.getInt(Key.FETCH_SIZE))
                            .timeOut(taskConf.getLong(Key.QUERY_TIMEOUT_MS))
                            .build();
                    treeSession.open();
                }
            } catch (IoTDBConnectionException | IllegalArgumentException e) {
                closeSessions();
                throw DataXException.asDataXException(IoTDBReaderErrorCode.CONNECTION_FAILED,
                        "Failed to open the IoTDB reader session.", e);
            }
        }

        @Override
        public void startRead(RecordSender recordSender) {
            List<String> queries = IoTDBReaderConfig.queries(taskConf);
            int timeColumnPosition = taskConf.getInt(Key.TIME_COLUMN_POSITION);
            long queryTimeoutMs = taskConf.getLong(Key.QUERY_TIMEOUT_MS);
            boolean tableModel = "table".equals(taskConf.getString(Key.MODEL));

            for (String query : queries) {
                try (SessionDataSet dataSet = tableModel
                        ? tableSession.executeQueryStatement(query, queryTimeoutMs)
                        : treeSession.executeQueryStatement(query, queryTimeoutMs)) {
                    List<String> columnNames = dataSet.getColumnNames();
                    List<String> columnTypes = dataSet.getColumnTypes();
                    while (dataSet.hasNext()) {
                        RowRecord row = dataSet.next();
                        Record record = recordSender.createRecord();
                        try {
                            IoTDBReaderRecordMapper.append(row, record, columnNames, columnTypes,
                                    timeColumnPosition);
                            recordSender.sendToWriter(record);
                        } catch (RuntimeException e) {
                            super.getTaskPluginCollector().collectDirtyRecord(record, e);
                            LOG.warn("Skipped an IoTDB row that could not be converted to a DataX record.", e);
                        }
                    }
                } catch (IoTDBConnectionException | StatementExecutionException e) {
                    throw DataXException.asDataXException(IoTDBReaderErrorCode.CONNECTION_FAILED,
                            "IoTDB query failed.", e);
                }
            }
        }

        @Override
        public void destroy() {
            closeSessions();
        }

        private void closeSessions() {
            try {
                if (tableSession != null) {
                    tableSession.close();
                    tableSession = null;
                }
                if (treeSession != null) {
                    treeSession.close();
                    treeSession = null;
                }
            } catch (IoTDBConnectionException e) {
                LOG.warn("Failed to close the IoTDB reader session cleanly.", e);
            }
        }
    }
}
