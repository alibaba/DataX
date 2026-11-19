package com.alibaba.datax.plugin.writer.iotdbwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.TableSessionBuilder;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IoTDBWriter extends Writer {

    public static class Job extends Writer.Job {
        private Configuration jobConf;

        @Override
        public void init() {
            jobConf = super.getPluginJobConf();
            IoTDBWriterConfig.validateAndSetDefaults(jobConf);
        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> result = new ArrayList<>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                result.add(jobConf.clone());
            }
            return result;
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration taskConf;
        private IoTDBWriterConfig writerConfig;
        private Session treeSession;
        private ITableSession tableSession;
        private TaskPluginCollector collector;
        private List<String> preSqls;

        @Override
        public void init() {
            taskConf = super.getPluginJobConf();
            writerConfig = IoTDBWriterConfig.validateAndSetDefaults(taskConf);
            collector = super.getTaskPluginCollector();
            preSqls = taskConf.getList(Key.PRE_SQL, String.class);
            if (preSqls == null) {
                preSqls = Collections.emptyList();
            }

            try {
                if ("table".equals(writerConfig.model)) {
                    TableSessionBuilder builder = new TableSessionBuilder()
                            .nodeUrls(Collections.singletonList(
                                    taskConf.getString(Key.HOST) + ":" + taskConf.getInt(Key.PORT)))
                            .username(taskConf.getString(Key.USERNAME))
                            .password(taskConf.getString(Key.PASSWORD))
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
                            .timeOut(taskConf.getLong(Key.QUERY_TIMEOUT_MS))
                            .build();
                    treeSession.open();
                }
            } catch (IoTDBConnectionException | IllegalArgumentException e) {
                closeSessions();
                throw DataXException.asDataXException(IoTDBWriterErrorCode.CONNECTION_FAILED,
                        "Failed to open the IoTDB writer session.", e);
            }
        }

        @Override
        public void prepare() {
            for (String sql : preSqls) {
                if (sql == null || sql.trim().isEmpty()) {
                    throw DataXException.asDataXException(IoTDBWriterErrorCode.ILLEGAL_VALUE,
                            "preSql cannot contain an empty statement.");
                }
                try {
                    if (tableSession != null) {
                        tableSession.executeNonQueryStatement(sql);
                    } else {
                        treeSession.executeNonQueryStatement(sql);
                    }
                } catch (IoTDBConnectionException | StatementExecutionException e) {
                    throw DataXException.asDataXException(IoTDBWriterErrorCode.WRITE_FAILED,
                            "An IoTDB preSql statement failed.", e);
                }
            }
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            List<Record> batch = new ArrayList<>(writerConfig.batchSize);
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    batch.add(record);
                    if (batch.size() >= writerConfig.batchSize) {
                        flush(batch);
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    flush(batch);
                }
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw DataXException.asDataXException(IoTDBWriterErrorCode.WRITE_FAILED,
                        "IoTDB rejected a write batch.", e);
            }
        }

        private void flush(List<Record> records)
                throws IoTDBConnectionException, StatementExecutionException {
            Tablet tablet = IoTDBWriterRecordMapper.newTablet(writerConfig, records.size());
            int rowIndex = 0;
            for (Record record : records) {
                try {
                    IoTDBWriterRecordMapper.append(record, tablet, writerConfig, rowIndex);
                    rowIndex++;
                } catch (RuntimeException e) {
                    collector.collectDirtyRecord(record, e);
                }
            }
            tablet.setRowSize(rowIndex);
            if (rowIndex == 0) {
                return;
            }
            if (tableSession != null) {
                tableSession.insert(tablet);
            } else {
                treeSession.insertTablet(tablet);
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
                LOG.warn("Failed to close the IoTDB writer session cleanly.", e);
            }
        }
    }
}
