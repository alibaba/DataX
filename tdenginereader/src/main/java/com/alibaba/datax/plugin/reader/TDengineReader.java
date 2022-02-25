package com.alibaba.datax.plugin.reader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.tdenginewriter.Key;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TDengineReader extends Reader {

    private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            // check username
            String username = this.originalConfig.getString(Key.USERNAME);
            if (StringUtils.isBlank(username))
                throw DataXException.asDataXException(TDengineReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.USERNAME + "] is not set.");

            // check password
            String password = this.originalConfig.getString(Key.PASSWORD);
            if (StringUtils.isBlank(password))
                throw DataXException.asDataXException(TDengineReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.PASSWORD + "] is not set.");

            // check connection
            List<Object> connection = this.originalConfig.getList(Key.CONNECTION);
            if (connection == null || connection.isEmpty())
                throw DataXException.asDataXException(TDengineReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.CONNECTION + "] is not set.");
            for (int i = 0; i < connection.size(); i++) {
                Configuration conn = Configuration.from(connection.get(i).toString());
                List<Object> table = conn.getList(Key.TABLE);
                if (table == null || table.isEmpty())
                    throw DataXException.asDataXException(TDengineReaderErrorCode.REQUIRED_VALUE,
                            "The parameter [" + Key.TABLE + "] of connection[" + (i + 1) + "] is not set.");
                String jdbcUrl = conn.getString(Key.JDBC_URL);
                if (StringUtils.isBlank(jdbcUrl))
                    throw DataXException.asDataXException(TDengineReaderErrorCode.REQUIRED_VALUE,
                            "The parameter [" + Key.JDBC_URL + "] of connection[" + (i + 1) + "] is not set.");
            }
            // check column
            List<Object> column = this.originalConfig.getList(Key.COLUMN);
            if (column == null || column.isEmpty())
                throw DataXException.asDataXException(TDengineReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.CONNECTION + "] is not set or is empty.");

            SimpleDateFormat format = new SimpleDateFormat(DATETIME_FORMAT);
            // check beginDateTime
            String beginDatetime = this.originalConfig.getString(Key.BEGIN_DATETIME);
            long start = Long.MIN_VALUE;
            if (!StringUtils.isBlank(beginDatetime)) {
                try {
                    start = format.parse(beginDatetime).getTime();
                } catch (ParseException e) {
                    throw DataXException.asDataXException(TDengineReaderErrorCode.ILLEGAL_VALUE,
                            "The parameter [" + Key.BEGIN_DATETIME + "] needs to conform to the [" + DATETIME_FORMAT + "] format.");
                }
            }
            // check endDateTime
            String endDatetime = this.originalConfig.getString(Key.END_DATETIME);
            long end = Long.MAX_VALUE;
            if (!StringUtils.isBlank(endDatetime)) {
                try {
                    end = format.parse(endDatetime).getTime();
                } catch (ParseException e) {
                    throw DataXException.asDataXException(TDengineReaderErrorCode.ILLEGAL_VALUE,
                            "The parameter [" + Key.END_DATETIME + "] needs to conform to the [" + DATETIME_FORMAT + "] format.");
                }
            }
            if (start >= end)
                throw DataXException.asDataXException(TDengineReaderErrorCode.ILLEGAL_VALUE,
                        "The parameter [" + Key.BEGIN_DATETIME + "] should be less than the parameter [" + Key.END_DATETIME + "].");

        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> configurations = new ArrayList<>();

            List<Object> connectionList = this.originalConfig.getList(Key.CONNECTION);
            for (Object conn : connectionList) {
                Configuration clone = this.originalConfig.clone();
                Configuration conf = Configuration.from(conn.toString());
                String jdbcUrl = conf.getString(Key.JDBC_URL);
                clone.set(Key.JDBC_URL, jdbcUrl);
                clone.set(Key.TABLE, conf.getList(Key.TABLE));
                clone.remove(Key.CONNECTION);
                configurations.add(clone);
            }

            LOG.info("Configuration: {}", configurations);
            return configurations;
        }
    }

    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration readerSliceConfig;
        private Connection conn;
        private List<String> tables;
        private List<String> columns;

        private String startTime;
        private String endTime;

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            LOG.info("getPluginJobConf: {}", readerSliceConfig);

            String url = readerSliceConfig.getString(Key.JDBC_URL);
            if (StringUtils.isBlank(url))
                throw DataXException.asDataXException(TDengineReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.JDBC_URL + "] is not set.");
            String user = readerSliceConfig.getString(Key.USERNAME);
            String password = readerSliceConfig.getString(Key.PASSWORD);
            try {
                this.conn = DriverManager.getConnection(url, user, password);
            } catch (SQLException e) {
                throw DataXException.asDataXException(TDengineReaderErrorCode.CONNECTION_FAILED,
                        "The parameter [" + Key.JDBC_URL + "] : " + url + " failed to connect since: " + e.getMessage(), e);
            }

            this.tables = readerSliceConfig.getList(Key.TABLE, String.class);
            this.columns = readerSliceConfig.getList(Key.COLUMN, String.class);
            this.startTime = readerSliceConfig.getString(Key.BEGIN_DATETIME);
            this.endTime = readerSliceConfig.getString(Key.END_DATETIME);
        }

        @Override
        public void destroy() {

        }

        @Override
        public void startRead(RecordSender recordSender) {

            try (Statement stmt = conn.createStatement()) {
                for (String table : tables) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("select ").append(StringUtils.join(columns, ","))
                            .append(" from ").append(table).append(" ");

                    if (StringUtils.isBlank(startTime)) {
                        sb.append("where _c0 >= ").append(Long.MIN_VALUE);
                    } else {
                        sb.append("where _c0 >= '").append(startTime).append("'");
                    }
                    if (!StringUtils.isBlank(endTime)) {
                        sb.append(" and _c0 < '").append(endTime).append("'");
                    }

                    String sql = sb.toString();
                    ResultSet rs = stmt.executeQuery(sql);
                    while (rs.next()) {
                        Record record = buildRecord(recordSender, rs, "UTF-8");
                        recordSender.sendToWriter(record);
                    }
                }
            } catch (SQLException e) {
                throw DataXException.asDataXException(TDengineReaderErrorCode.RUNTIME_EXCEPTION, e.getMessage(), e);
            } finally {
                try {
                    if (conn != null)
                        conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        private Record buildRecord(RecordSender recordSender, ResultSet rs, String mandatoryEncoding) {
            Record record = recordSender.createRecord();
            try {
                ResultSetMetaData metaData = rs.getMetaData();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    int columnType = metaData.getColumnType(i);
                    switch (columnType) {
                        case Types.SMALLINT:
                        case Types.TINYINT:
                        case Types.INTEGER:
                        case Types.BIGINT:
                            record.addColumn(new LongColumn(rs.getString(i)));
                            break;
                        case Types.FLOAT:
                        case Types.DOUBLE:
                            record.addColumn(new DoubleColumn(rs.getString(i)));
                            break;
                        case Types.BOOLEAN:
                            record.addColumn(new BoolColumn(rs.getBoolean(i)));
                            break;
                        case Types.TIMESTAMP:
                            record.addColumn(new DateColumn(rs.getTimestamp(i)));
                            break;
                        case Types.BINARY:
                            record.addColumn(new BytesColumn(rs.getBytes(i)));
                            break;
                        case Types.NCHAR:
                            String rawData;
                            if (StringUtils.isBlank(mandatoryEncoding)) {
                                rawData = rs.getString(i);
                            } else {
                                rawData = new String((rs.getBytes(i) == null ? new byte[0] : rs.getBytes(i)), mandatoryEncoding);
                            }
                            record.addColumn(new StringColumn(rawData));
                            break;
                    }
                }
            } catch (SQLException | UnsupportedEncodingException e) {
                throw DataXException.asDataXException(TDengineReaderErrorCode.ILLEGAL_VALUE, "获取或发送数据点的过程中出错！", e);
            }
            return record;
        }
    }

    private static Long parseSplitInterval(String splitInterval) throws Exception {
        final long second = 1000;
        final long minute = 60 * second;
        final long hour = 60 * minute;
        final long day = 24 * hour;

        Pattern compile = Pattern.compile("^(\\d+)([dhms])$");
        Matcher matcher = compile.matcher(splitInterval);
        while (matcher.find()) {
            long value = Long.parseLong(matcher.group(1));
            if (value == 0)
                throw new Exception("invalid splitInterval: 0");
            char unit = matcher.group(2).charAt(0);
            switch (unit) {
                case 'd':
                    return value * day;
                default:
                case 'h':
                    return value * hour;
                case 'm':
                    return value * minute;
                case 's':
                    return value * second;
            }
        }
        throw new Exception("invalid splitInterval: " + splitInterval);
    }

}
