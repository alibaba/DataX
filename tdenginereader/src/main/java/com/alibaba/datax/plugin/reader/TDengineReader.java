package com.alibaba.datax.plugin.reader;

import com.alibaba.datax.common.constant.CommonConstant;
import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.fastjson.JSON;
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
            // check user
            String user = this.originalConfig.getString(Key.USER);
            if (StringUtils.isBlank(user))
                throw DataXException.asDataXException(TDengineReaderErrorCode.REQUIRED_VALUE, "The parameter [" + Key.USER + "] is not set.");

            // check password
            String password = this.originalConfig.getString(Key.PASSWORD);
            if (StringUtils.isBlank(password))
                throw DataXException.asDataXException(TDengineReaderErrorCode.REQUIRED_VALUE, "The parameter [" + Key.PASSWORD + "] is not set.");

            SimpleDateFormat format = new SimpleDateFormat(DATETIME_FORMAT);
            // check beginDateTime
            String beginDatetime = this.originalConfig.getString(Key.BEGIN_DATETIME);
            if (StringUtils.isBlank(beginDatetime))
                throw DataXException.asDataXException(TDengineReaderErrorCode.REQUIRED_VALUE, "The parameter [" + Key.BEGIN_DATETIME + "] is not set.");
            Long start;
            try {
                start = format.parse(beginDatetime).getTime();
            } catch (ParseException e) {
                throw DataXException.asDataXException(TDengineReaderErrorCode.ILLEGAL_VALUE, "The parameter [" + Key.BEGIN_DATETIME + "] needs to conform to the [" + DATETIME_FORMAT + "] format.");
            }

            // check endDateTime
            String endDatetime = this.originalConfig.getString(Key.END_DATETIME);
            if (StringUtils.isBlank(endDatetime))
                throw DataXException.asDataXException(TDengineReaderErrorCode.REQUIRED_VALUE, "The parameter [" + Key.END_DATETIME + "] is not set.");
            Long end;
            try {
                end = format.parse(endDatetime).getTime();
            } catch (ParseException e) {
                throw DataXException.asDataXException(TDengineReaderErrorCode.ILLEGAL_VALUE, "The parameter [" + Key.END_DATETIME + "] needs to conform to the [" + DATETIME_FORMAT + "] format.");
            }
            if (start >= end)
                throw DataXException.asDataXException(TDengineReaderErrorCode.ILLEGAL_VALUE, "The parameter [" + Key.BEGIN_DATETIME + "] should be less than the parameter [" + Key.END_DATETIME + "].");

            // check splitInterval
            String splitInterval = this.originalConfig.getString(Key.SPLIT_INTERVAL);
            Long split;
            if (StringUtils.isBlank(splitInterval))
                throw DataXException.asDataXException(TDengineReaderErrorCode.REQUIRED_VALUE, "The parameter [" + Key.SPLIT_INTERVAL + "] is not set.");
            try {
                split = parseSplitInterval(splitInterval);
            } catch (Exception e) {
                throw DataXException.asDataXException(TDengineReaderErrorCode.ILLEGAL_VALUE, "The parameter [" + Key.SPLIT_INTERVAL + "] should be like: \"123d|h|m|s\", error: " + e.getMessage());
            }

            this.originalConfig.set(Key.BEGIN_DATETIME, start);
            this.originalConfig.set(Key.END_DATETIME, end);
            this.originalConfig.set(Key.SPLIT_INTERVAL, split);

            // check connection
            List<Object> connection = this.originalConfig.getList(Key.CONNECTION);
            if (connection == null || connection.isEmpty())
                throw DataXException.asDataXException(TDengineReaderErrorCode.REQUIRED_VALUE, "The parameter [" + Key.CONNECTION + "] is not set.");
            for (int i = 0; i < connection.size(); i++) {
                Configuration conn = Configuration.from(connection.get(i).toString());
                List<Object> table = conn.getList(Key.TABLE);
                if (table == null || table.isEmpty())
                    throw DataXException.asDataXException(TDengineReaderErrorCode.REQUIRED_VALUE, "The parameter [" + Key.TABLE + "] of connection[" + (i + 1) + "] is not set.");
                String jdbcUrl = conn.getString(Key.JDBC_URL);
                if (StringUtils.isBlank(jdbcUrl))
                    throw DataXException.asDataXException(TDengineReaderErrorCode.REQUIRED_VALUE, "The parameter [" + Key.JDBC_URL + "] of connection[" + (i + 1) + "] is not set.");
            }

            // check column
            List<Object> column = this.originalConfig.getList(Key.COLUMN);
            if (column == null || column.isEmpty())
                throw DataXException.asDataXException(TDengineReaderErrorCode.REQUIRED_VALUE, "The parameter [" + Key.CONNECTION + "] is not set or is empty.");
        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> configurations = new ArrayList<>();
            // do split
            Long start = this.originalConfig.getLong(Key.BEGIN_DATETIME);
            Long end = this.originalConfig.getLong(Key.END_DATETIME);
            Long split = this.originalConfig.getLong(Key.SPLIT_INTERVAL);

            List<Object> conns = this.originalConfig.getList(Key.CONNECTION);

            for (Long ts = start; ts < end; ts += split) {
                for (int i = 0; i < conns.size(); i++) {
                    Configuration clone = this.originalConfig.clone();
                    clone.remove(Key.SPLIT_INTERVAL);

                    clone.set(Key.BEGIN_DATETIME, ts);
                    clone.set(Key.END_DATETIME, Math.min(ts + split, end));

                    Configuration conf = Configuration.from(conns.get(i).toString());
                    String jdbcUrl = conf.getString(Key.JDBC_URL);
                    clone.set(Key.JDBC_URL, jdbcUrl);
                    clone.set(Key.TABLE, conf.getList(Key.TABLE));

                    // 抽取 jdbcUrl 中的 ip/port 进行资源使用的打标，以提供给 core 做有意义的 shuffle 操作
                    clone.set(CommonConstant.LOAD_BALANCE_RESOURCE_MARK, DataBaseType.parseIpFromJdbcUrl(jdbcUrl));
                    clone.remove(Key.CONNECTION);

                    configurations.add(clone);
                    LOG.info("Configuration: {}", JSON.toJSONString(clone));
                }
            }
            return configurations;
        }
    }

    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private Connection conn;

        private Long startTime;
        private Long endTime;
        private List<String> tables;
        private List<String> columns;
        private String mandatoryEncoding;

        @Override
        public void init() {
            Configuration readerSliceConfig = super.getPluginJobConf();
            LOG.info("getPluginJobConf: {}", JSON.toJSONString(readerSliceConfig));

            String url = readerSliceConfig.getString(Key.JDBC_URL);
            if (StringUtils.isBlank(url))
                throw DataXException.asDataXException(TDengineReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.JDBC_URL + "] is not set.");

            tables = readerSliceConfig.getList(Key.TABLE, String.class);
            columns = readerSliceConfig.getList(Key.COLUMN, String.class);

            String user = readerSliceConfig.getString(Key.USER);
            String password = readerSliceConfig.getString(Key.PASSWORD);

            try {
                conn = DriverManager.getConnection(url, user, password);
            } catch (SQLException e) {
                throw DataXException.asDataXException(TDengineReaderErrorCode.CONNECTION_FAILED,
                        "The parameter [" + Key.JDBC_URL + "] : " + url + " failed to connect since: " + e.getMessage());
            }

            this.mandatoryEncoding = readerSliceConfig.getString(Key.MANDATORY_ENCODING, "");

            startTime = readerSliceConfig.getLong(Key.BEGIN_DATETIME);
            endTime = readerSliceConfig.getLong(Key.END_DATETIME);
        }


        @Override
        public void destroy() {

        }

        @Override
        public void startRead(RecordSender recordSender) {
            try (Statement stmt = conn.createStatement()) {
                for (int i = 0; i < tables.size(); i++) {
                    String sql = "select " + StringUtils.join(columns, ",") + " from " + tables.get(i) + " where _c0 >= " + startTime + " and _c0 < " + endTime;
                    ResultSet rs = stmt.executeQuery(sql);
                    ResultSetMetaData metaData = rs.getMetaData();
                    while (rs.next()) {
                        transportOneRecord(recordSender, rs, metaData, metaData.getColumnCount(), this.mandatoryEncoding);
                    }
                }
            } catch (SQLException e) {
                throw DataXException.asDataXException(TDengineReaderErrorCode.ILLEGAL_VALUE, "获取或发送数据点的过程中出错！", e);
            } finally {
                try {
                    if (conn != null)
                        conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        private Record transportOneRecord(RecordSender recordSender, ResultSet rs, ResultSetMetaData metaData, int columnCount, String mandatoryEncoding) {
            Record record = buildRecord(recordSender, rs, metaData, columnCount, mandatoryEncoding);
            recordSender.sendToWriter(record);
            return record;
        }

        private Record buildRecord(RecordSender recordSender, ResultSet rs, ResultSetMetaData metaData, int columnCount, String mandatoryEncoding) {
            Record record = recordSender.createRecord();

            try {
                for (int i = 1; i <= columnCount; i++) {
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

    private static final long second = 1000;
    private static final long minute = 60 * second;
    private static final long hour = 60 * minute;
    private static final long day = 24 * hour;

    private static Long parseSplitInterval(String splitInterval) throws Exception {
        Pattern compile = Pattern.compile("^(\\d+)([dhms])$");
        Matcher matcher = compile.matcher(splitInterval);
        while (matcher.find()) {
            Long value = Long.valueOf(matcher.group(1));
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
