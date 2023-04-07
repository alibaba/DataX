package com.alibaba.datax.plugin.reader.tsdbreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.tsdbreader.conn.TSDBConnection;
import com.alibaba.datax.plugin.reader.tsdbreader.util.TimeUtils;
import com.alibaba.fastjson2.JSON;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：TSDB Reader
 *
 * @author Benedict Jin
 * @since 2019-10-21
 */
@SuppressWarnings("unused")
public class TSDBReader extends Reader {

    public static class Job extends Reader.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            String type = originalConfig.getString(Key.SINK_DB_TYPE, Key.TYPE_DEFAULT_VALUE);
            if (StringUtils.isBlank(type)) {
                throw DataXException.asDataXException(
                        TSDBReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.SINK_DB_TYPE + "] is not set.");
            }
            if (!Key.TYPE_SET.contains(type)) {
                throw DataXException.asDataXException(
                        TSDBReaderErrorCode.ILLEGAL_VALUE,
                        "The parameter [" + Key.SINK_DB_TYPE + "] should be one of [" +
                                JSON.toJSONString(Key.TYPE_SET) + "].");
            }

            String address = originalConfig.getString(Key.ENDPOINT);
            if (StringUtils.isBlank(address)) {
                throw DataXException.asDataXException(
                        TSDBReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.ENDPOINT + "] is not set.");
            }

            String username = originalConfig.getString(Key.USERNAME, null);
            if (StringUtils.isBlank(username)) {
                LOG.warn("The parameter [" + Key.USERNAME + "] is blank.");
            }
            String password = originalConfig.getString(Key.PASSWORD, null);
            if (StringUtils.isBlank(password)) {
                LOG.warn("The parameter [" + Key.PASSWORD + "] is blank.");
            }

            // tagK / field could be empty
            if ("TSDB".equals(type)) {
                List<String> columns = originalConfig.getList(Key.COLUMN, String.class);
                if (columns == null || columns.isEmpty()) {
                    throw DataXException.asDataXException(
                            TSDBReaderErrorCode.REQUIRED_VALUE,
                            "The parameter [" + Key.COLUMN + "] is not set.");
                }
            } else {
                List<String> columns = originalConfig.getList(Key.COLUMN, String.class);
                if (columns == null || columns.isEmpty()) {
                    throw DataXException.asDataXException(
                            TSDBReaderErrorCode.REQUIRED_VALUE,
                            "The parameter [" + Key.COLUMN + "] is not set.");
                }
                for (String specifyKey : Constant.MUST_CONTAINED_SPECIFY_KEYS) {
                    boolean containSpecifyKey = false;
                    for (String column : columns) {
                        if (column.startsWith(specifyKey)) {
                            containSpecifyKey = true;
                            break;
                        }
                    }
                    if (!containSpecifyKey) {
                        throw DataXException.asDataXException(
                                TSDBReaderErrorCode.ILLEGAL_VALUE,
                                "The parameter [" + Key.COLUMN + "] should contain "
                                        + JSON.toJSONString(Constant.MUST_CONTAINED_SPECIFY_KEYS) + ".");
                    }
                }
                final List<String> metrics = originalConfig.getList(Key.METRIC, String.class);
                if (metrics == null || metrics.isEmpty()) {
                    throw DataXException.asDataXException(
                            TSDBReaderErrorCode.REQUIRED_VALUE,
                            "The parameter [" + Key.METRIC + "] is not set.");
                }
            }

            Integer splitIntervalMs = originalConfig.getInt(Key.INTERVAL_DATE_TIME,
                    Key.INTERVAL_DATE_TIME_DEFAULT_VALUE);
            if (splitIntervalMs <= 0) {
                throw DataXException.asDataXException(
                        TSDBReaderErrorCode.ILLEGAL_VALUE,
                        "The parameter [" + Key.INTERVAL_DATE_TIME + "] should be great than zero.");
            }

            Boolean isCombine = originalConfig.getBool(Key.COMBINE, Key.COMBINE_DEFAULT_VALUE);

            SimpleDateFormat format = new SimpleDateFormat(Constant.DEFAULT_DATA_FORMAT);
            String startTime = originalConfig.getString(Key.BEGIN_DATE_TIME);
            Long startDate;
            if (startTime == null || startTime.trim().length() == 0) {
                throw DataXException.asDataXException(
                        TSDBReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.BEGIN_DATE_TIME + "] is not set.");
            } else {
                try {
                    startDate = format.parse(startTime).getTime();
                } catch (ParseException e) {
                    throw DataXException.asDataXException(TSDBReaderErrorCode.ILLEGAL_VALUE,
                            "The parameter [" + Key.BEGIN_DATE_TIME +
                                    "] needs to conform to the [" + Constant.DEFAULT_DATA_FORMAT + "] format.");
                }
            }
            String endTime = originalConfig.getString(Key.END_DATE_TIME);
            Long endDate;
            if (endTime == null || endTime.trim().length() == 0) {
                throw DataXException.asDataXException(
                        TSDBReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.END_DATE_TIME + "] is not set.");
            } else {
                try {
                    endDate = format.parse(endTime).getTime();
                } catch (ParseException e) {
                    throw DataXException.asDataXException(TSDBReaderErrorCode.ILLEGAL_VALUE,
                            "The parameter [" + Key.END_DATE_TIME +
                                    "] needs to conform to the [" + Constant.DEFAULT_DATA_FORMAT + "] format.");
                }
            }
            if (startDate >= endDate) {
                throw DataXException.asDataXException(TSDBReaderErrorCode.ILLEGAL_VALUE,
                        "The parameter [" + Key.BEGIN_DATE_TIME +
                                "] should be less than the parameter [" + Key.END_DATE_TIME + "].");
            }
        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> configurations = new ArrayList<>();

            // get metrics
            String type = originalConfig.getString(Key.SINK_DB_TYPE, Key.TYPE_DEFAULT_VALUE);
            List<String> columns4TSDB = null;
            List<String> columns4RDB = null;
            List<String> metrics = null;
            if ("TSDB".equals(type)) {
                columns4TSDB = originalConfig.getList(Key.COLUMN, String.class);
            } else {
                columns4RDB = originalConfig.getList(Key.COLUMN, String.class);
                metrics = originalConfig.getList(Key.METRIC, String.class);
            }

            // get time interval
            Integer splitIntervalMs = originalConfig.getInt(Key.INTERVAL_DATE_TIME,
                    Key.INTERVAL_DATE_TIME_DEFAULT_VALUE);

            // get time range
            SimpleDateFormat format = new SimpleDateFormat(Constant.DEFAULT_DATA_FORMAT);
            long startTime;
            try {
                startTime = format.parse(originalConfig.getString(Key.BEGIN_DATE_TIME)).getTime();
            } catch (ParseException e) {
                throw DataXException.asDataXException(
                        TSDBReaderErrorCode.ILLEGAL_VALUE, "Analysis [" + Key.BEGIN_DATE_TIME + "] failed.", e);
            }
            long endTime;
            try {
                endTime = format.parse(originalConfig.getString(Key.END_DATE_TIME)).getTime();
            } catch (ParseException e) {
                throw DataXException.asDataXException(
                        TSDBReaderErrorCode.ILLEGAL_VALUE, "Analysis [" + Key.END_DATE_TIME + "] failed.", e);
            }
            if (TimeUtils.isSecond(startTime)) {
                startTime *= 1000;
            }
            if (TimeUtils.isSecond(endTime)) {
                endTime *= 1000;
            }
            DateTime startDateTime = new DateTime(TimeUtils.getTimeInHour(startTime));
            DateTime endDateTime = new DateTime(TimeUtils.getTimeInHour(endTime));

            final Boolean isCombine = originalConfig.getBool(Key.COMBINE, Key.COMBINE_DEFAULT_VALUE);

            if ("TSDB".equals(type)) {
                if (isCombine) {
                    // split by time in hour
                    while (startDateTime.isBefore(endDateTime)) {
                        Configuration clone = this.originalConfig.clone();
                        clone.set(Key.COLUMN, columns4TSDB);

                        clone.set(Key.BEGIN_DATE_TIME, startDateTime.getMillis());
                        startDateTime = startDateTime.plusMillis(splitIntervalMs);
                        // Make sure the time interval is [start, end).
                        clone.set(Key.END_DATE_TIME, startDateTime.getMillis() - 1);
                        configurations.add(clone);

                        LOG.info("Configuration: {}", JSON.toJSONString(clone));
                    }
                } else {
                    // split by time in hour
                    while (startDateTime.isBefore(endDateTime)) {
                        // split by metric
                        for (String column : columns4TSDB) {
                            Configuration clone = this.originalConfig.clone();
                            clone.set(Key.COLUMN, Collections.singletonList(column));

                            clone.set(Key.BEGIN_DATE_TIME, startDateTime.getMillis());
                            startDateTime = startDateTime.plusMillis(splitIntervalMs);
                            // Make sure the time interval is [start, end).
                            clone.set(Key.END_DATE_TIME, startDateTime.getMillis() - 1);
                            configurations.add(clone);

                            LOG.info("Configuration: {}", JSON.toJSONString(clone));
                        }
                    }
                }
            } else {
                if (isCombine) {
                    while (startDateTime.isBefore(endDateTime)) {
                        Configuration clone = this.originalConfig.clone();
                        clone.set(Key.COLUMN, columns4RDB);
                        clone.set(Key.METRIC, metrics);

                        clone.set(Key.BEGIN_DATE_TIME, startDateTime.getMillis());
                        startDateTime = startDateTime.plusMillis(splitIntervalMs);
                        // Make sure the time interval is [start, end).
                        clone.set(Key.END_DATE_TIME, startDateTime.getMillis() - 1);
                        configurations.add(clone);

                        LOG.info("Configuration: {}", JSON.toJSONString(clone));
                    }
                } else {
                    // split by time in hour
                    while (startDateTime.isBefore(endDateTime)) {
                        // split by metric
                        for (String metric : metrics) {
                            Configuration clone = this.originalConfig.clone();
                            clone.set(Key.COLUMN, columns4RDB);
                            clone.set(Key.METRIC, Collections.singletonList(metric));

                            clone.set(Key.BEGIN_DATE_TIME, startDateTime.getMillis());
                            startDateTime = startDateTime.plusMillis(splitIntervalMs);
                            // Make sure the time interval is [start, end).
                            clone.set(Key.END_DATE_TIME, startDateTime.getMillis() - 1);
                            configurations.add(clone);

                            LOG.info("Configuration: {}", JSON.toJSONString(clone));
                        }
                    }
                }
            }
            return configurations;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Reader.Task {

        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private String type;
        private List<String> columns4TSDB = null;
        private List<String> columns4RDB = null;
        private List<String> metrics = null;
        private Map<String, Object> fields;
        private Map<String, Object> tags;
        private TSDBConnection conn;
        private Long startTime;
        private Long endTime;
        private Boolean isCombine;
        private Map<String, Object> hint;

        @Override
        public void init() {
            Configuration readerSliceConfig = super.getPluginJobConf();

            LOG.info("getPluginJobConf: {}", JSON.toJSONString(readerSliceConfig));

            this.type = readerSliceConfig.getString(Key.SINK_DB_TYPE);
            if ("TSDB".equals(type)) {
                columns4TSDB = readerSliceConfig.getList(Key.COLUMN, String.class);
            } else {
                columns4RDB = readerSliceConfig.getList(Key.COLUMN, String.class);
                metrics = readerSliceConfig.getList(Key.METRIC, String.class);
            }
            this.fields = readerSliceConfig.getMap(Key.FIELD);
            this.tags = readerSliceConfig.getMap(Key.TAG);

            String address = readerSliceConfig.getString(Key.ENDPOINT);
            String username = readerSliceConfig.getString(Key.USERNAME);
            String password = readerSliceConfig.getString(Key.PASSWORD);

            conn = new TSDBConnection(address, username, password);

            this.startTime = readerSliceConfig.getLong(Key.BEGIN_DATE_TIME);
            this.endTime = readerSliceConfig.getLong(Key.END_DATE_TIME);

            this.isCombine = readerSliceConfig.getBool(Key.COMBINE, Key.COMBINE_DEFAULT_VALUE);
            this.hint = readerSliceConfig.getMap(Key.HINT);
        }

        @Override
        public void prepare() {
        }

        @Override
        @SuppressWarnings("unchecked")
        public void startRead(RecordSender recordSender) {
            try {
                if ("TSDB".equals(type)) {
                    for (String metric : columns4TSDB) {
                        final Map<String, String> tags = this.tags == null ?
                                null : (Map<String, String>) this.tags.get(metric);
                        if (fields == null || !fields.containsKey(metric)) {
                            conn.sendDPs(metric, tags, this.startTime, this.endTime, recordSender, hint);
                        } else {
                            conn.sendDPs(metric, (List<String>) fields.get(metric),
                                    tags, this.startTime, this.endTime, recordSender, hint);
                        }
                    }
                } else {
                    if (isCombine) {
                        final Map<String, String> tags = this.tags == null ?
                                null : (Map<String, String>) this.tags.get(metrics.get(0));
                        conn.sendRecords(metrics, tags, startTime, endTime, columns4RDB, recordSender, hint);
                    } else {
                        for (String metric : metrics) {
                            final Map<String, String> tags = this.tags == null ?
                                    null : (Map<String, String>) this.tags.get(metric);
                            if (fields == null || !fields.containsKey(metric)) {
                                conn.sendRecords(metric, tags, startTime, endTime, columns4RDB, isCombine, recordSender, hint);
                            } else {
                                conn.sendRecords(metric, (List<String>) fields.get(metric),
                                        tags, startTime, endTime, columns4RDB, recordSender, hint);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        TSDBReaderErrorCode.ILLEGAL_VALUE, "Error in getting or sending data point！", e);
            }
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }
}
