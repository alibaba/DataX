package com.alibaba.datax.plugin.reader.influxdbreader;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.influxdbreader.util.TimeUtils;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.fastjson.JSON;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.QueryApi;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.joda.time.DateTime;
import java.util.Map;

public class InfluxDBReader extends Reader {
    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            // check connection
            List<Configuration> connectionList = this.originalConfig.getListConfiguration(Key.CONNECTION);
            if (connectionList == null || connectionList.isEmpty()) {
                throw DataXException.asDataXException(InfluxDBReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.CONNECTION + "] is not set.");
            }
            for (int i = 0; i < connectionList.size(); i++) {
                Configuration conn = connectionList.get(i);
                // check url
                String urlList = conn.getString(Key.URL);
                if (StringUtils.isBlank(urlList)) {
                    throw DataXException.asDataXException(InfluxDBReaderErrorCode.REQUIRED_VALUE,
                            "The parameter [" + Key.URL + "] of connection + [" + (i + 1) + "] is not set.");
                }
                // check token
                String token = conn.getString(Key.TOKEN);
                if (StringUtils.isBlank(token)) {
                    throw DataXException.asDataXException(InfluxDBReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.TOKEN + "] is not set.");
                }
                // check org
                String org = conn.getString(Key.TOKEN);
                if (StringUtils.isBlank(org)) {
                    throw DataXException.asDataXException(InfluxDBReaderErrorCode.REQUIRED_VALUE,
                            "The parameter [" + Key.ORG + "] is not set.");
                }
                // check bucket
                String bucket = conn.getString(Key.BUCKET);
                if (StringUtils.isBlank(bucket)) {
                    throw DataXException.asDataXException(InfluxDBReaderErrorCode.REQUIRED_VALUE,
                            "The parameter [" + Key.BUCKET + "] is not set.");
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
                    throw DataXException.asDataXException(InfluxDBReaderErrorCode.ILLEGAL_VALUE,
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
                    throw DataXException.asDataXException(InfluxDBReaderErrorCode.ILLEGAL_VALUE,
                            "The parameter [" + Key.END_DATETIME + "] needs to conform to the [" + DATETIME_FORMAT + "] format.");
                }
            }
            if (start >= end) {
                throw DataXException.asDataXException(InfluxDBReaderErrorCode.ILLEGAL_VALUE,
                        "The parameter " + Key.BEGIN_DATETIME + ": " + beginDatetime + " should be less than the parameter " + Key.END_DATETIME + ": " + endDatetime + ".");
            }
            // check intervalTime
            Integer splitIntervalMs = this.originalConfig.getInt(Key.INTERVAL_DATE_TIME,
                    Key.INTERVAL_DATE_TIME_DEFAULT_VALUE);
            if (splitIntervalMs <= 0) {
                throw DataXException.asDataXException(
                        InfluxDBReaderErrorCode.ILLEGAL_VALUE,
                        "The parameter [" + Key.INTERVAL_DATE_TIME + "] should be great than zero.");
            }
        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> configurations = new ArrayList<>();
            // get time interval
            Integer splitIntervalH = this.originalConfig.getInt(Key.INTERVAL_DATE_TIME,
                    Key.INTERVAL_DATE_TIME_DEFAULT_VALUE);
            // get time range
            SimpleDateFormat format = new SimpleDateFormat(Constant.DEFAULT_DATA_FORMAT);
            long startTime;
            try {
                startTime = format.parse(originalConfig.getString(Key.BEGIN_DATETIME)).getTime();
            } catch (ParseException e) {
                throw DataXException.asDataXException(
                        InfluxDBReaderErrorCode.ILLEGAL_VALUE, "Analysis [" + Key.BEGIN_DATETIME + "] failed.", e);
            }
            long endTime;
            try {
                endTime = format.parse(originalConfig.getString(Key.END_DATETIME)).getTime();
            } catch (ParseException e) {
                throw DataXException.asDataXException(
                        InfluxDBReaderErrorCode.ILLEGAL_VALUE, "Analysis [" + Key.END_DATETIME + "] failed.", e);
            }
            // split
            while (startTime < endTime) {
                Configuration clone = this.originalConfig.clone();
                clone.set(Key.BEGIN_DATETIME, (startTime / 1000));
                startTime = startTime + splitIntervalH * 60 * 60 * 1000;
                // Make sure the time interval is [start, end).
                clone.set(Key.END_DATETIME, (startTime / 1000 - 1));
                configurations.add(clone);
                LOG.info("Configuration: {}", JSON.toJSONString(clone));
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

        /**
         * measurement list
         */
        private List<String> measurements;

        /**
         *  map: [measurement,tagKeys]
         */
        private Map<String, List<String>> measurementTagKeysMap;
        private Configuration readerSliceConfig;
        private InfluxDBClient influxDBClient;
        private String url;
        private char[] token;
        private String org;
        private String bucket;
        private long startTime;
        private long endTime;

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            LOG.info("getPluginJobConf: {}", JSON.toJSONString(readerSliceConfig));

            this.measurementTagKeysMap = new HashMap<>();

            List<Configuration> connectionList = this.readerSliceConfig.getListConfiguration(Key.CONNECTION);
            Configuration conn = connectionList.get(0);
            this.url = conn.getString(Key.URL);
            this.token = conn.getString(Key.TOKEN).toCharArray();
            this.org = conn.getString(Key.ORG);
            this.bucket = conn.getString(Key.BUCKET);

            this.influxDBClient = InfluxDBClientFactory.create(url, token, org, bucket);

            this.startTime = this.readerSliceConfig.getLong(Key.BEGIN_DATETIME);
            this.endTime = this.readerSliceConfig.getLong(Key.END_DATETIME);
        }

        @Override
        public void prepare() {
            // 1. build connection
            InfluxDBClient influxDBClient = InfluxDBClientFactory.create(url, token, org, bucket);
            // 2. query measurement
            measurements = new ArrayList<>();
            // 2.1 generate query flux
            StringBuilder queryMeasurementsFlux = new StringBuilder();
            queryMeasurementsFlux.append("import \"influxdata/influxdb/schema\"\n");
            queryMeasurementsFlux.append("\n");
            queryMeasurementsFlux.append("schema.measurements(bucket: \"");
            queryMeasurementsFlux.append(bucket);
            queryMeasurementsFlux.append("\")");
            // 2.2 query
            QueryApi queryMeasurementsApi = influxDBClient.getQueryApi();
            List<FluxTable> measurementsTables = queryMeasurementsApi.query(queryMeasurementsFlux.toString());
            for (FluxTable fluxTable : measurementsTables) {
                List<FluxRecord> records = fluxTable.getRecords();
                for (FluxRecord fluxRecord : records) {
                    measurements.add(String.valueOf(fluxRecord.getValueByKey("_value")));
                }
            }
            // 3. query tagKeys
            for (String measurement : measurements) {
                // 3.1 generate tagKeys query flux
                StringBuilder queryTagKeysFlux = new StringBuilder();
                queryTagKeysFlux.append("import \"influxdata/influxdb/schema\"\n");
                queryTagKeysFlux.append("\n");
                queryTagKeysFlux.append("schema.measurementTagKeys(\n");
                queryTagKeysFlux.append("    bucket: \"");
                queryTagKeysFlux.append(bucket);
                queryTagKeysFlux.append("\",\n");
                queryTagKeysFlux.append("    measurement: \"");
                queryTagKeysFlux.append(measurement);
                queryTagKeysFlux.append("\",\n)");
                // 3.2 query
                List<String> tagKeys = new ArrayList<>();
                measurementTagKeysMap.put(measurement, tagKeys);
                QueryApi queryTagKeysApi = influxDBClient.getQueryApi();
                List<FluxTable> TagKeysTables = queryTagKeysApi.query(queryTagKeysFlux.toString());
                for (FluxTable fluxTable : TagKeysTables) {
                    List<FluxRecord> records = fluxTable.getRecords();
                    // 固定从下标 4 开始，为第一个 tagKey
                    for (int i = 4; i < records.size(); i++) {
                        tagKeys.add(String.valueOf(records.get(i).getValueByKey("_value")));
                    }
                }
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public void startRead(RecordSender recordSender) {
            for (String measurement : measurements) {
                // 1. 读取 [startTime, endTime] 范围内的数据
                // 1.1 generate query flux
                StringBuilder queryFlux = new StringBuilder();
                queryFlux.append("from(bucket:\"");
                queryFlux.append(this.bucket);
                queryFlux.append("\") |> range(start: ");
                // startTime
                queryFlux.append(this.startTime);
                queryFlux.append(", stop: ");
                queryFlux.append(this.endTime);
                queryFlux.append(") |> filter(fn: (r) => r._measurement == \"");
                // measurement
                queryFlux.append(measurement);
                queryFlux.append("\")");

                LOG.info("queryFlux:" + queryFlux.toString());
                // TODO intervalTime 需要进一步拆分: 1day
                // 2. query and write to DataX record
                QueryApi queryApi = influxDBClient.getQueryApi();
                List<FluxTable> tables = queryApi.query(queryFlux.toString());
                for (FluxTable fluxTable : tables) {
                    List<FluxRecord> records = fluxTable.getRecords();
                    for (FluxRecord fluxRecord : records) {
                        Record record = recordSender.createRecord();
                        // TODO time 精度？
                        long time = fluxRecord.getTime().getLong(ChronoField.NANO_OF_SECOND);
                        LongColumn timeColumn = new LongColumn(time);
                        record.addColumn(timeColumn);
                        try {
                            Column fieldValueColumn = getColumn(fluxRecord.getValues().get("_value"));
                            record.addColumn(fieldValueColumn);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        String fieldKey = fluxRecord.getField();
                        StringColumn fieldKeyColumn = new StringColumn(fieldKey);
                        record.addColumn(fieldKeyColumn);

                        StringColumn measurementColumn = new StringColumn(measurement);
                        record.addColumn(measurementColumn);

                        // obtain TagKeys
                        List<String> tagKeys = measurementTagKeysMap.get(measurement);
                        Map<String, Object> values = fluxRecord.getValues();
                        for (String tagKey : tagKeys) {
                            StringColumn tagKeyColumn = new StringColumn(tagKey);
                            record.addColumn(tagKeyColumn);

                            String tagValue = String.valueOf(values.getOrDefault(tagKey, "@"));
                            StringColumn tagValueColumn = new StringColumn(tagValue);
                            record.addColumn(tagValueColumn);
                        }

                        // 3. send to writer
                        recordSender.sendToWriter(record);
                    }
                }
            }
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

        /**
         * 判断 value 数据类型，并返回相应 column
         * @param value
         * @return
         * @throws Exception
         */
        private static Column getColumn(Object value) throws Exception {
            Column valueColumn;
            if (value instanceof Double) {
                valueColumn = new DoubleColumn((Double) value);
            } else if (value instanceof Long) {
                valueColumn = new LongColumn((Long) value);
            } else if (value instanceof String) {
                valueColumn = new StringColumn((String) value);
            } else if (value instanceof Integer) {
                valueColumn = new LongColumn(((Integer)value).longValue());
            } else {
                throw new Exception(String.format("value not supported type: [%s]", value.getClass().getSimpleName()));
            }
            return valueColumn;
        }

    }
}
