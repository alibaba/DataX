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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
            List<Configuration> connectionList = this.originalConfig.getListConfiguration(Key.CONNECTION);
            if (connectionList == null || connectionList.isEmpty())
                throw DataXException.asDataXException(TDengineReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.CONNECTION + "] is not set.");
            for (int i = 0; i < connectionList.size(); i++) {
                Configuration conn = connectionList.get(i);
                // check jdbcUrl
                List<Object> jdbcUrlList = conn.getList(Key.JDBC_URL);
                if (jdbcUrlList == null || jdbcUrlList.isEmpty()) {
                    throw DataXException.asDataXException(TDengineReaderErrorCode.REQUIRED_VALUE,
                            "The parameter [" + Key.JDBC_URL + "] of connection[" + (i + 1) + "] is not set.");
                }
                // check table/querySql
                List<Object> querySqlList = conn.getList(Key.QUERY_SQL);
                if (querySqlList == null || querySqlList.isEmpty()) {
                    String querySql = conn.getString(Key.QUERY_SQL);
                    if (StringUtils.isBlank(querySql)) {
                        List<Object> table = conn.getList(Key.TABLE);
                        if (table == null || table.isEmpty())
                            throw DataXException.asDataXException(TDengineReaderErrorCode.REQUIRED_VALUE,
                                    "The parameter [" + Key.TABLE + "] of connection[" + (i + 1) + "] is not set.");
                    }
                }
            }

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

            List<Configuration> connectionList = this.originalConfig.getListConfiguration(Key.CONNECTION);
            for (Configuration conn : connectionList) {
                List<String> jdbcUrlList = conn.getList(Key.JDBC_URL, String.class);
                for (String jdbcUrl : jdbcUrlList) {
                    Configuration clone = this.originalConfig.clone();
                    clone.set(Key.JDBC_URL, jdbcUrl);
                    clone.set(Key.TABLE, conn.getList(Key.TABLE));
                    clone.set(Key.QUERY_SQL, conn.getList(Key.QUERY_SQL));
                    clone.remove(Key.CONNECTION);
                    configurations.add(clone);
                }
            }

            LOG.info("Configuration: {}", configurations);
            return configurations;
        }
    }

    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration readerSliceConfig;
        private String mandatoryEncoding;
        private Connection conn;

        private List<String> tables;
        private List<String> columns;
        private String startTime;
        private String endTime;
        private String where;
        private List<String> querySql;

        static {
            try {
                Class.forName("com.taosdata.jdbc.TSDBDriver");
                Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
            } catch (ClassNotFoundException ignored) {
                LOG.warn(ignored.getMessage(), ignored);
            }
        }

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();

            String user = readerSliceConfig.getString(Key.USERNAME);
            String password = readerSliceConfig.getString(Key.PASSWORD);

            String url = readerSliceConfig.getString(Key.JDBC_URL);
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
            this.where = readerSliceConfig.getString(Key.WHERE, "_c0 > " + Long.MIN_VALUE);
            this.querySql = readerSliceConfig.getList(Key.QUERY_SQL, String.class);
            this.mandatoryEncoding = readerSliceConfig.getString(Key.MANDATORY_ENCODING, "UTF-8");
        }

        @Override
        public void destroy() {
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                LOG.error(e.getMessage(), e);
            }
        }

        @Override
        public void startRead(RecordSender recordSender) {
            List<String> sqlList = new ArrayList<>();

            if (querySql == null || querySql.isEmpty()) {
                for (String table : tables) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("select ").append(StringUtils.join(columns, ",")).append(" from ").append(table).append(" ");
                    sb.append("where ").append(where);
                    if (!StringUtils.isBlank(startTime)) {
                        sb.append(" and _c0 >= '").append(startTime).append("'");
                    }
                    if (!StringUtils.isBlank(endTime)) {
                        sb.append(" and _c0 < '").append(endTime).append("'");
                    }
                    String sql = sb.toString().trim();
                    sqlList.add(sql);
                }
            } else {
                sqlList.addAll(querySql);
            }

            for (String sql : sqlList) {
                try (Statement stmt = conn.createStatement()) {
                    ResultSet rs = stmt.executeQuery(sql);
                    while (rs.next()) {
                        Record record = buildRecord(recordSender, rs, mandatoryEncoding);
                        recordSender.sendToWriter(record);
                    }
                } catch (SQLException e) {
                    LOG.error(e.getMessage(), e);
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
            } catch (SQLException e) {
                throw DataXException.asDataXException(TDengineReaderErrorCode.ILLEGAL_VALUE, "database query error！", e);
            } catch (UnsupportedEncodingException e) {
                throw DataXException.asDataXException(TDengineReaderErrorCode.ILLEGAL_VALUE, "illegal mandatoryEncoding", e);
            }
            return record;
        }
    }


}
