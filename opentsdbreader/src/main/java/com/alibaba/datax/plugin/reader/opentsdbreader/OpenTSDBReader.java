package com.alibaba.datax.plugin.reader.opentsdbreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.conn.OpenTSDBConnection;
import com.alibaba.datax.plugin.reader.util.TimeUtils;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：Key
 *
 * @author Benedict Jin
 * @since 2019-04-18
 */
@SuppressWarnings("unused")
public class OpenTSDBReader extends Reader {

    public static class Job extends Reader.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            String address = originalConfig.getString(Key.ENDPOINT);
            if (StringUtils.isBlank(address)) {
                throw DataXException.asDataXException(
                        OpenTSDBReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.ENDPOINT + "] is not set.");
            }

            List<String> columns = originalConfig.getList(Key.COLUMN, String.class);
            if (columns == null || columns.isEmpty()) {
                throw DataXException.asDataXException(
                        OpenTSDBReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.COLUMN + "] is not set.");
            }

            SimpleDateFormat format = new SimpleDateFormat(Constant.DEFAULT_DATA_FORMAT);
            String startTime = originalConfig.getString(Key.BEGIN_DATE_TIME);
            Long startDate;
            if (startTime == null || startTime.trim().length() == 0) {
                throw DataXException.asDataXException(
                        OpenTSDBReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.BEGIN_DATE_TIME + "] is not set.");
            } else {
                try {
                    startDate = format.parse(startTime).getTime();
                } catch (ParseException e) {
                    throw DataXException.asDataXException(OpenTSDBReaderErrorCode.ILLEGAL_VALUE,
                            "The parameter [" + Key.BEGIN_DATE_TIME +
                                    "] needs to conform to the [" + Constant.DEFAULT_DATA_FORMAT + "] format.");
                }
            }
            String endTime = originalConfig.getString(Key.END_DATE_TIME);
            Long endDate;
            if (endTime == null || endTime.trim().length() == 0) {
                throw DataXException.asDataXException(
                        OpenTSDBReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.END_DATE_TIME + "] is not set.");
            } else {
                try {
                    endDate = format.parse(endTime).getTime();
                } catch (ParseException e) {
                    throw DataXException.asDataXException(OpenTSDBReaderErrorCode.ILLEGAL_VALUE,
                            "The parameter [" + Key.END_DATE_TIME +
                                    "] needs to conform to the [" + Constant.DEFAULT_DATA_FORMAT + "] format.");
                }
            }
            if (startDate >= endDate) {
                throw DataXException.asDataXException(OpenTSDBReaderErrorCode.ILLEGAL_VALUE,
                        "The parameter [" + Key.BEGIN_DATE_TIME +
                                "] should be less than the parameter [" + Key.END_DATE_TIME + "].");
            }
        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> configurations = new ArrayList<Configuration>();

            // get metrics
            List<String> columns = originalConfig.getList(Key.COLUMN, String.class);

            // get time range
            SimpleDateFormat format = new SimpleDateFormat(Constant.DEFAULT_DATA_FORMAT);
            long startTime;
            try {
                startTime = format.parse(originalConfig.getString(Key.BEGIN_DATE_TIME)).getTime();
            } catch (ParseException e) {
                throw DataXException.asDataXException(
                        OpenTSDBReaderErrorCode.ILLEGAL_VALUE, "解析[" + Key.BEGIN_DATE_TIME + "]失败.", e);
            }
            long endTime;
            try {
                endTime = format.parse(originalConfig.getString(Key.END_DATE_TIME)).getTime();
            } catch (ParseException e) {
                throw DataXException.asDataXException(
                        OpenTSDBReaderErrorCode.ILLEGAL_VALUE, "解析[" + Key.END_DATE_TIME + "]失败.", e);
            }
            if (TimeUtils.isSecond(startTime)) {
                startTime *= 1000;
            }
            if (TimeUtils.isSecond(endTime)) {
                endTime *= 1000;
            }
            DateTime startDateTime = new DateTime(TimeUtils.getTimeInHour(startTime));
            DateTime endDateTime = new DateTime(TimeUtils.getTimeInHour(endTime));

            // split by metric
            for (String column : columns) {
                // split by time in hour
                while (startDateTime.isBefore(endDateTime)) {
                    Configuration clone = this.originalConfig.clone();
                    clone.set(Key.COLUMN, Collections.singletonList(column));

                    clone.set(Key.BEGIN_DATE_TIME, startDateTime.getMillis());
                    startDateTime = startDateTime.plusHours(1);
                    // Make sure the time interval is [start, end).
                    // Because net.opentsdb.core.Query.setEndTime means less than or equal to the end time.
                    clone.set(Key.END_DATE_TIME, startDateTime.getMillis() - 1);
                    configurations.add(clone);

                    LOG.info("Configuration: {}", JSON.toJSONString(clone));
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

        private List<String> columns;
        private OpenTSDBConnection conn;
        private Long startTime;
        private Long endTime;

        @Override
        public void init() {
            Configuration readerSliceConfig = super.getPluginJobConf();

            LOG.info("getPluginJobConf: {}", JSON.toJSONString(readerSliceConfig));

            this.columns = readerSliceConfig.getList(Key.COLUMN, String.class);
            String address = readerSliceConfig.getString(Key.ENDPOINT);

            conn = new OpenTSDBConnection(address);

            this.startTime = readerSliceConfig.getLong(Key.BEGIN_DATE_TIME);
            this.endTime = readerSliceConfig.getLong(Key.END_DATE_TIME);
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startRead(RecordSender recordSender) {
            try {
                for (String column : columns) {
                    conn.sendDPs(column, this.startTime, this.endTime, recordSender);
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        OpenTSDBReaderErrorCode.ILLEGAL_VALUE, "获取或发送数据点的过程中出错！", e);
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
