package com.alibaba.datax.plugin.writer.influxdb2writer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


/**
 * @author changl
 * @version 1.0
 * @date 2025/6/3
 */
public class Influxdb2Writer extends Writer {

    public static class Job extends Writer.Job {

        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            if (StringUtils.isBlank(originalConfig.getString(Key.URL))) {
                throw DataXException.asDataXException(Influxdb2WriterErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.URL + "] is not set.");
            }

            if (StringUtils.isBlank(originalConfig.getString(Key.TOKEN))) {
                throw DataXException.asDataXException(Influxdb2WriterErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.TOKEN + "] is not set.");
            }
            if (StringUtils.isBlank(originalConfig.getString(Key.ORG))) {
                throw DataXException.asDataXException(Influxdb2WriterErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.ORG + "] is not set.");
            }

            if (StringUtils.isBlank(originalConfig.getString(Key.BUCKET))) {
                throw DataXException.asDataXException(Influxdb2WriterErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.BUCKET + "] is not set.");
            }

            if (StringUtils.isBlank(originalConfig.getString(Key.MEASUREMENT))) {
                throw DataXException.asDataXException(Influxdb2WriterErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.MEASUREMENT + "] is not set.");
            }

            if (StringUtils.isBlank(originalConfig.getString(Key.FIELDS))) {
                throw DataXException.asDataXException(Influxdb2WriterErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.FIELDS + "] is not set.");
            }

            if (StringUtils.isBlank(originalConfig.getString(Key.BATCH_SIZE))) {
                originalConfig.set(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
            }

            if (Objects.isNull(originalConfig.getBool(Key.HAS_TS))) {
                originalConfig.set(Key.HAS_TS, Constant.DEFAULT_HAS_TS);
            }

            if (StringUtils.isBlank(originalConfig.getString(Key.TS_FORMAT))) {
                originalConfig.set(Key.TS_FORMAT, Constant.DEFAULT_TS_FORMAT);
            }

        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            ArrayList<Configuration> configurations = new ArrayList<Configuration>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(this.originalConfig.clone());
            }
            return configurations;
        }
    }

    public static class Task extends Writer.Task {

        private InfluxDBClient client;

        private WriteApi writeApi;

        private String measurement;

        private List<String> tags;

        private List<String> fields;

        private boolean hasTs;

        private DateTimeFormatter dateTimeFormatter;

        private Integer batchSize;

        @Override
        public void init() {
            Configuration pluginJobConf = getPluginJobConf();
            measurement = pluginJobConf.getString(Key.MEASUREMENT);
            tags = pluginJobConf.getList(Key.TAGS, String.class);
            fields = pluginJobConf.getList(Key.FIELDS, String.class);
            hasTs = pluginJobConf.getBool(Key.HAS_TS);
            dateTimeFormatter = DateTimeFormatter.ofPattern(pluginJobConf.getString(Key.TS_FORMAT));
            batchSize = pluginJobConf.getInt(Key.BATCH_SIZE);
            client = InfluxDBClientFactory.create(pluginJobConf.getString(Key.URL), pluginJobConf.getString(Key.TOKEN).toCharArray(), pluginJobConf.getString(Key.ORG), pluginJobConf.getString(Key.BUCKET));
            writeApi = client.makeWriteApi();
        }

        @Override
        public void destroy() {
            writeApi.close();
            client.close();
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            // 构建记录缓存
            List<Point> points = new ArrayList<>(batchSize);

            try {
                Record record;
                while ((record = lineReceiver.getFromReader()) != null) {
                    Point point = Point.measurement(measurement);

                    if (record.getColumnNumber() != fields.size() + tags.size() + 1) {
                        throw DataXException.asDataXException(Influxdb2WriterErrorCode.RUNTIME_EXCEPTION, " number of record fields:" + record.getColumnNumber() + " number of configuration fields:" + fields.size());
                    }

                    for (int i = 0; i < tags.size(); i++) {
                        String tag = tags.get(i);
                        Column column = record.getColumn(i);
                        point.addTag(tag, column.asString());
                    }
                    for (int i = 0; i < fields.size(); i++) {
                        String field = fields.get(i);
                        Column column = record.getColumn(tags.size() + i);
                        point.addField(field, column.asString());
                    }
                    if (hasTs) {
                        Column column = record.getColumn(record.getColumnNumber() - 1);
                        LocalDateTime time = LocalDateTime.from(dateTimeFormatter.parse(column.asString()));
                        point.time(time.atZone(ZoneId.systemDefault()).toInstant(), WritePrecision.S);
                    } else {
                        point.time(Instant.now(), WritePrecision.S);
                    }


                    points.add(point);
                    // 批量写入
                    if (points.size() >= batchSize) {
                        write(points);
                        points.clear();
                    }
                }

                // 写入剩余数据
                if (!points.isEmpty()) {
                    write(points);
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(Influxdb2WriterErrorCode.RUNTIME_EXCEPTION, "写入InfluxDB失败", e);
            }
        }

        private void write(List<Point> points) {
            writeApi.writePoints(points);
        }

    }

}
