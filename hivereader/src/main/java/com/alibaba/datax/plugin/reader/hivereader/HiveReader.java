package com.alibaba.datax.plugin.reader.hivereader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;


public class HiveReader extends Reader {

    /**
     * init -> prepare -> split
     */
    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration readerOriginConfig;

        private List<String> sqls;

        @Override
        public void init() {
            LOG.info("init() begin...");
            this.readerOriginConfig = super.getPluginJobConf();
            this.validate();
            LOG.info("init() ok and end...");
        }

        public void validate() {
            readerOriginConfig.getNecessaryValue(Key.HIVE_SQL, HiveReaderErrorCode.SQL_NOT_FOUND_ERROR);
            sqls = readerOriginConfig.getList(Key.HIVE_SQL, String.class);

            if (sqls == null || sqls.size() == 0) {
                throw DataXException.asDataXException(HiveReaderErrorCode.SQL_NOT_FOUND_ERROR,
                        "您未配置hive sql");
            } else {
                readerOriginConfig.getNecessaryValue(Key.JDBC_URL, HiveReaderErrorCode.REQUIRED_VALUE);
                readerOriginConfig.getNecessaryValue(Key.USER, HiveReaderErrorCode.REQUIRED_VALUE);
                readerOriginConfig.getNecessaryValue(Key.PASSWORD, HiveReaderErrorCode.REQUIRED_VALUE);

                if (readerOriginConfig.getBool(Key.HAVE_KERBEROS, false)) {
                    readerOriginConfig.getNecessaryValue(Key.KERBEROS_PRINCIPAL, HiveReaderErrorCode.REQUIRED_VALUE);
                    readerOriginConfig.getNecessaryValue(Key.KERBEROS_KEYTAB_FILE_PATH, HiveReaderErrorCode.REQUIRED_VALUE);
                }
            }
        }

        @Override
        public void prepare() {
            LOG.info(String.format("您即将执行的SQL条数为: [%s], 列表为: [%s]",
                    this.sqls.size(),
                    StringUtils.join(this.sqls, ";")));
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            LOG.info("split() begin...");
            List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();
            // 自定义SQL部分同步通过切分sql条数实现split
            List<List<String>> splitedSqls = this.splitSourceFiles(sqls, adviceNumber);
            for (List<String> sqls : splitedSqls) {
                Configuration splitedConfig = this.readerOriginConfig.clone();
                splitedConfig.set(Key.HIVE_SQL, sqls);
                readerSplitConfigs.add(splitedConfig);
            }
            return readerSplitConfigs;
        }

        private <T> List<List<T>> splitSourceFiles(final List<T> sourceList, int adviceNumber) {
            List<List<T>> splitedList = new ArrayList<List<T>>();
            int averageLength = sourceList.size() / adviceNumber;
            averageLength = averageLength == 0 ? 1 : averageLength;

            for (int begin = 0, end = 0; begin < sourceList.size(); begin = end) {
                end = begin + averageLength;
                if (end > sourceList.size()) {
                    end = sourceList.size();
                }
                splitedList.add(sourceList.subList(begin, end));
            }
            return splitedList;
        }

        @Override
        public void destroy() {

        }

    }

    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);
        private static final boolean IS_DEBUG = LOG.isDebugEnabled();
        protected final byte[] EMPTY_CHAR_ARRAY = new byte[0];

        private Configuration taskConfig;
        private String basicMsg;

        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startRead(RecordSender recordSender) {
            LOG.info("read start");
            List<String> querySqls = taskConfig.getList(Key.HIVE_SQL, String.class);
            String user = taskConfig.getString(Key.USER);
            String pass = taskConfig.getString(Key.PASSWORD);
            String jdbcUrl = taskConfig.getString(Key.JDBC_URL);

            basicMsg = String.format("jdbcUrl:[%s], taskId:[%s]", jdbcUrl, getTaskId());
            PerfTrace.getInstance().addTaskDetails(getTaskId(), basicMsg);

            Connection connection = DBUtil.getConnection(jdbcUrl, user, pass, taskConfig);
            ResultSet rs = null;
            try {
                for (String sql : querySqls) {
                    LOG.info("Begin to read record by Sql: [{}\n] {}.",
                            sql, basicMsg);
                    PerfRecord queryPerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.SQL_QUERY);
                    queryPerfRecord.start();

                    long rsNextUsedTime = 0;
                    long lastTime = System.nanoTime();
                    rs = DBUtil.query(connection, sql);
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnNumber = metaData.getColumnCount();
                    while (rs.next()) {
                        rsNextUsedTime += (System.nanoTime() - lastTime);
                        transportOneRecord(recordSender, rs, metaData,
                                columnNumber, "", getTaskPluginCollector());
                        lastTime = System.nanoTime();
                    }
                    queryPerfRecord.end(rsNextUsedTime);
                    LOG.info("Finished read record by Sql: [{}\n] {}.",
                            sql, basicMsg);
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(HiveReaderErrorCode.SQL_EXECUTE_FAIL,
                        "执行的SQL为: " + querySqls + " 具体错误信息为：" + e);
            } finally {
                DBUtil.closeDBResources(rs, null, connection);
            }
        }

        @Override
        public void destroy() {

        }

        protected Record transportOneRecord(RecordSender recordSender, ResultSet rs,
                                            ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding,
                                            TaskPluginCollector taskPluginCollector) {
            Record record = buildRecord(recordSender, rs, metaData, columnNumber, mandatoryEncoding, taskPluginCollector);
            recordSender.sendToWriter(record);
            return record;
        }

        protected Record buildRecord(RecordSender recordSender, ResultSet rs, ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding,
                                     TaskPluginCollector taskPluginCollector) {
            Record record = recordSender.createRecord();

            try {
                for (int i = 1; i <= columnNumber; i++) {
                    switch (metaData.getColumnType(i)) {

                        case Types.CHAR:
                        case Types.NCHAR:
                        case Types.VARCHAR:
                        case Types.LONGVARCHAR:
                        case Types.NVARCHAR:
                        case Types.LONGNVARCHAR:
                            String rawData;
                            if (StringUtils.isBlank(mandatoryEncoding)) {
                                rawData = rs.getString(i);
                            } else {
                                rawData = new String((rs.getBytes(i) == null ? EMPTY_CHAR_ARRAY :
                                        rs.getBytes(i)), mandatoryEncoding);
                            }
                            record.addColumn(new StringColumn(rawData));
                            break;

                        case Types.CLOB:
                        case Types.NCLOB:
                            record.addColumn(new StringColumn(rs.getString(i)));
                            break;

                        case Types.SMALLINT:
                        case Types.TINYINT:
                        case Types.INTEGER:
                        case Types.BIGINT:
                            record.addColumn(new LongColumn(rs.getString(i)));
                            break;

                        case Types.NUMERIC:
                        case Types.DECIMAL:
                            record.addColumn(new DoubleColumn(rs.getString(i)));
                            break;

                        case Types.FLOAT:
                        case Types.REAL:
                        case Types.DOUBLE:
                            record.addColumn(new DoubleColumn(rs.getString(i)));
                            break;

                        case Types.TIME:
                            record.addColumn(new DateColumn(rs.getTime(i)));
                            break;

                        // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
                        case Types.DATE:
                            if (metaData.getColumnTypeName(i).equalsIgnoreCase("year")) {
                                record.addColumn(new LongColumn(rs.getInt(i)));
                            } else {
                                record.addColumn(new DateColumn(rs.getDate(i)));
                            }
                            break;

                        case Types.TIMESTAMP:
                            record.addColumn(new DateColumn(rs.getTimestamp(i)));
                            break;

                        case Types.BINARY:
                        case Types.VARBINARY:
                        case Types.BLOB:
                        case Types.LONGVARBINARY:
                            record.addColumn(new BytesColumn(rs.getBytes(i)));
                            break;

                        // warn: bit(1) -> Types.BIT 可使用BoolColumn
                        // warn: bit(>1) -> Types.VARBINARY 可使用BytesColumn
                        case Types.BOOLEAN:
                        case Types.BIT:
                            record.addColumn(new BoolColumn(rs.getBoolean(i)));
                            break;

                        // 针对hive中特有的复杂类型,转为string类型输出
                        case Types.ARRAY:
                        case Types.STRUCT:
                        case Types.JAVA_OBJECT:
                        case Types.OTHER:
                            String arrStr = (String) rs.getObject(i);
                            record.addColumn(new StringColumn(arrStr));
                            break;

                        case Types.NULL:
                            String stringData = null;
                            if (rs.getObject(i) != null) {
                                stringData = rs.getObject(i).toString();
                            }
                            record.addColumn(new StringColumn(stringData));
                            break;

                        default:
                            throw DataXException
                                    .asDataXException(
                                            HiveReaderErrorCode.UNSUPPORTED_TYPE,
                                            String.format(
                                                    "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. 字段名:[%s], 字段名称:[%s], 字段Java类型:[%s]. 请尝试使用数据库函数将其转换datax支持的类型 或者不同步该字段 .",
                                                    metaData.getColumnName(i),
                                                    metaData.getColumnType(i),
                                                    metaData.getColumnClassName(i)));
                    }
                }
            } catch (Exception e) {
                if (IS_DEBUG) {
                    LOG.debug("read data " + record.toString()
                            + " occur exception:", e);
                }
                //TODO 这里识别为脏数据靠谱吗？
                taskPluginCollector.collectDirtyRecord(record, e);
                if (e instanceof DataXException) {
                    throw (DataXException) e;
                }
            }
            return record;
        }

    }
}
