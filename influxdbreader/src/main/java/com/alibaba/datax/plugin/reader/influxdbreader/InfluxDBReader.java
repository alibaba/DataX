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
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.fastjson.JSON;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.QueryApi;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class InfluxDBReader extends Reader {
    private static Map<String, List<String>> measurementTagKeysMap;

    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
        private Configuration originalConfig;
        private List<Integer> compeletedTaskIds;
        private InfluxDBClient influxDBClient;
        private String url;
        private char[] token;
        private String org;
        private String bucket;

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
                url = conn.getString(Key.URL);
                if (StringUtils.isBlank(url)) {
                    throw DataXException.asDataXException(InfluxDBReaderErrorCode.REQUIRED_VALUE,
                            "The parameter [" + Key.URL + "] of connection + [" + (i + 1) + "] is not set.");
                }
                // check token
                String token = conn.getString(Key.TOKEN);
                if (StringUtils.isBlank(token)) {
                    throw DataXException.asDataXException(InfluxDBReaderErrorCode.REQUIRED_VALUE,
                            "The parameter [" + Key.TOKEN + "] is not set.");
                }
                this.token = token.toCharArray();
                // check org
                org = conn.getString(Key.ORG);
                if (StringUtils.isBlank(org)) {
                    throw DataXException.asDataXException(InfluxDBReaderErrorCode.REQUIRED_VALUE,
                            "The parameter [" + Key.ORG + "] is not set.");
                }
                // check bucket
                bucket = conn.getString(Key.BUCKET);
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

            // init connection
            influxDBClient = InfluxDBClientFactory.create(url, token, org, bucket);
        }

        @Override
        public void prepare() {
            // 1. 判断 log 文件是否存在
            File taskIdLog = new File(System.getProperty(Constant.DATAX_HOME) + Constant.DEFAULT_TASK_ID_LOG_RELATIVE_PATH);
            if (taskIdLog.exists()) {
                compeletedTaskIds = new ArrayList<>(1024);
                RandomAccessFile randomAccessFile = null;
                FileChannel channel = null;
                try {
                    randomAccessFile = new RandomAccessFile(System.getProperty(Constant.DATAX_HOME) + Constant.DEFAULT_TASK_ID_LOG_RELATIVE_PATH, "r");
                    channel = randomAccessFile.getChannel();
                    ByteBuffer buffer = ByteBuffer.allocate(1024);
                    int bytesRead = channel.read(buffer);
                    ByteBuffer stringBuffer = ByteBuffer.allocate(20);
                    while (bytesRead != -1) {
                        buffer.flip();
                        while (buffer.hasRemaining()) {
                            byte b = buffer.get();
                            // 换行或回车
                            if (b == 10 || b == 13) {
                                stringBuffer.flip();
                                // 读取一行
                                final String num = StandardCharsets.UTF_8.decode(stringBuffer).toString();
                                if (!"".equals(num)) {
                                    compeletedTaskIds.add(Integer.valueOf(num));
                                }
                                stringBuffer.clear();
                            } else {
                                if (stringBuffer.hasRemaining()) {
                                    stringBuffer.put(b);
                                }
                            }
                        }
                        buffer.clear();
                        // 继续往 buffer 中写
                        bytesRead = channel.read(buffer);
                    }
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    try {
                        channel.close();
                        randomAccessFile.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            // 2. cache schema
            // 2.1. build connection
            measurementTagKeysMap = new HashMap<>(1024);
            // 2. query measurement
            /**
             * measurement list
             */
            List<String> measurements = new ArrayList<>();
            // 2.1 generate query flux
            String queryMeasurementsFlux = "import \"influxdata/influxdb/schema\" schema.measurements(bucket: \"" +
                    bucket +
                    "\")";
            // 2.2 query
            QueryApi queryMeasurementsApi = influxDBClient.getQueryApi();
            LOG.info("queryMeasurementsApi:{}", queryMeasurementsFlux);
            List<FluxTable> measurementsTables = queryMeasurementsApi.query(queryMeasurementsFlux);
            for (FluxTable fluxTable : measurementsTables) {
                List<FluxRecord> records = fluxTable.getRecords();
                for (FluxRecord fluxRecord : records) {
                    measurements.add(String.valueOf(fluxRecord.getValueByKey("_value")));
                }
            }
            // 3. query tagKeys
            for (String measurement : measurements) {
                // 3.1 generate tagKeys query flux
                String queryTagKeysFlux = "import \"influxdata/influxdb/schema\"" +
                        " schema.measurementTagKeys(" +
                        "bucket: \"" +
                        bucket +
                        "\", measurement: \"" +
                        measurement +
                        "\")";
                // 3.2 query
                List<String> tagKeys = new ArrayList<>();
                measurementTagKeysMap.put(measurement, tagKeys);
                QueryApi queryTagKeysApi = influxDBClient.getQueryApi();
                LOG.info("queryTagKeysApi:{}", queryTagKeysFlux);
                List<FluxTable> tagKeysTables = queryTagKeysApi.query(queryTagKeysFlux);
                for (FluxTable fluxTable : tagKeysTables) {
                    List<FluxRecord> records = fluxTable.getRecords();
                    // 固定从下标 4 开始，为第一个 tagKey
                    for (int i = 4; i < records.size(); i++) {
                        tagKeys.add(String.valueOf(records.get(i).getValueByKey("_value")));
                    }
                }
            }
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> configurations = new ArrayList<>();
            // get time interval
            Integer splitIntervalHour = this.originalConfig.getInt(Key.INTERVAL_DATE_TIME,
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
            int taskId = 0;
            while (startTime < endTime) {
                Configuration clone = this.originalConfig.clone();
                clone.set(Key.BEGIN_DATETIME, startTime);
                startTime = startTime + splitIntervalHour * Constant.DEFAULT_HOUR_TO_MILLISECOND;
                // flux range 函数查询范围 [start, stop).
                clone.set(Key.END_DATETIME, startTime);
                // 切分时判断，如果已经在上次同步过了，则不加入到 list 中
                if (compeletedTaskIds == null) {
                    configurations.add(clone);
                } else if (!compeletedTaskIds.contains(taskId)) {
                    configurations.add(clone);
                }
                taskId++;
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
         * map: [measurement,tagKeys]
         */
        private InfluxDBClient influxDBClient;
        private String url;
        private char[] token;
        private String org;
        private String bucket;
        private long startTime;
        private long endTime;
        private long miniTaskIntervalSecond;

        @Override
        public void init() {
            Configuration readerSliceConfig = super.getPluginJobConf();
            LOG.info("getPluginJobConf: {}", JSON.toJSONString(readerSliceConfig));

//            this.measurementTagKeysMap = new HashMap<>(1024);

            List<Configuration> connectionList = readerSliceConfig.getListConfiguration(Key.CONNECTION);
            Configuration conn = connectionList.get(0);
            this.url = conn.getString(Key.URL);
            this.token = conn.getString(Key.TOKEN).toCharArray();
            this.org = conn.getString(Key.ORG);
            this.bucket = conn.getString(Key.BUCKET);

            int readTimeout = conn.getInt(Key.READE_TIMEOUT, Constant.DEFAULT_READ_TIMEOUT_SECOND);
            int writeTimeout = conn.getInt(Key.WRITE_TIMEOUT, Constant.DEFAULT_WRITE_TIMEOUT_SECOND);
            int connectTimeout = conn.getInt(Key.CONNECT_TIMEOUT, Constant.DEFAULT_CONNECT_TIMEOUT_SECOND);

            OkHttpClient.Builder okHttpClient = new OkHttpClient.Builder()
                    .readTimeout(readTimeout, TimeUnit.SECONDS)
                    .writeTimeout(writeTimeout, TimeUnit.SECONDS)
                    .connectTimeout(connectTimeout, TimeUnit.SECONDS);
            InfluxDBClientOptions options = InfluxDBClientOptions
                    .builder()
                    .url(url)
                    .authenticateToken(token)
                    .org(org)
                    .bucket(bucket)
                    .okHttpClient(okHttpClient)
                    .build();
            this.influxDBClient = InfluxDBClientFactory.create(options);

            this.startTime = readerSliceConfig.getLong(Key.BEGIN_DATETIME);
            this.endTime = readerSliceConfig.getLong(Key.END_DATETIME);
            this.miniTaskIntervalSecond = readerSliceConfig.getLong(Key.MINI_TASK_INTERVAL_SECOND, Constant.DEFAULT_HOUR_TO_MILLISECOND);
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startRead(RecordSender recordSender) {
            // 1. 计算切分数量
            QueryApi queryApi = influxDBClient.getQueryApi();
            while (startTime < endTime) {
                // 1. 毫秒转换成秒
                long queryStartTime = this.startTime / 1000;
                long nextTime = (this.startTime + this.miniTaskIntervalSecond * 1000) / 1000;
                long queryEndTime = nextTime <= endTime ? nextTime : endTime;
                // 1. 读取 [queryStartTime, queryEndTime] 范围内的数据
                // 1.1 generate query flux
                String queryFlux = "from(bucket:\""
                        + bucket
                        + "\") |> range(start: "
                        + queryStartTime
                        + ", stop: "
                        + queryEndTime
                        + ")";
                LOG.info("queryFlux:{}", queryFlux);
                // 2. query and write to DataX record
                List<FluxRecord> records;
                try {
                    List<FluxTable> tables = queryApi.query(queryFlux);
                    String measurement;
                    long time;
                    for (FluxTable fluxTable : tables) {
                        records = fluxTable.getRecords();
                        for (FluxRecord fluxRecord : records) {
                            measurement = fluxRecord.getMeasurement();
                            Record record = recordSender.createRecord();
                            time = fluxRecord.getTime().toEpochMilli();
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

                            // obtain TagKeys and TagValues
                            List<String> tagKeys = measurementTagKeysMap.get(measurement);
                            Map<String, Object> values = fluxRecord.getValues();
                            for (String tagKey : tagKeys) {
                                StringColumn tagKeyColumn = new StringColumn(tagKey);
                                record.addColumn(tagKeyColumn);

                                StringColumn tagValueColumn;
                                Object tagValueObj = values.get(tagKey);
                                if (tagValueObj != null) {
                                    tagValueColumn = new StringColumn(String.valueOf(tagValueObj));
                                } else {
                                    tagValueColumn = new StringColumn(null);
                                }
                                record.addColumn(tagValueColumn);
                            }
                            // 3. send to writer
                            recordSender.sendToWriter(record);
                        }
                    }
                } catch (Exception e) {
                    LOG.error("[startRead] [query range [{}, {}] failed]. Error Info:{}", queryStartTime, queryEndTime, e);
                }
                // 更新查询时间
                this.startTime += this.miniTaskIntervalSecond * 1000;
            }
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
            influxDBClient.close();
        }

        /**
         * 判断 value 数据类型，并返回相应 column
         *
         * @param value columnValue
         * @return column
         * @throws Exception exception
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
                valueColumn = new LongColumn(((Integer) value).longValue());
            } else {
                throw new Exception(String.format("value not supported type: [%s]", value.getClass().getSimpleName()));
            }
            return valueColumn;
        }
    }
}
