package com.alibaba.datax.plugin.writer.kafkawriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaWriter extends Writer {

    public static final String PARAMETER_BOOTSTRAP_SERVERS = "bootstrapServers";
    public static final String PARAMETER_TOPIC = "topic";
    public static final String PARAMETER_COLUMN = "column";
    public static final String PARAMETER_KEY_INDEX = "keyIndex";
    public static final String PARAMETER_PROPS = "props";

    public static class Job extends Writer.Job {
        private static final Logger log = LoggerFactory.getLogger(Job.class);

        private Configuration conf = null;

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
            log.debug("kafka writer job conf:{}", this.conf.toJSON());
            this.conf.getNecessaryValue(PARAMETER_BOOTSTRAP_SERVERS, KafkaWriterError.KAFKA_CONN_BOOTSTRAP_SERVERS_MISSING);
            this.conf.getNecessaryValue(PARAMETER_TOPIC, KafkaWriterError.KAFKA_CONN_TOPIC_MISSING);
            this.conf.getNecessaryValue(PARAMETER_COLUMN, KafkaWriterError.KAFKA_COLUMN_MISSING);
        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configList = new ArrayList<>();
            for (int i = 0; i < mandatoryNumber; i++) {
                configList.add(this.conf.clone());
            }
            return configList;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger log = LoggerFactory.getLogger(Task.class);

        private Configuration conf = null;
        private Producer<String, String> producer;

        private String topic;
        private List<String> columns;
        private int keyIndex = -1;

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
            topic = this.conf.getString(PARAMETER_TOPIC);
            columns = this.conf.getList(PARAMETER_COLUMN, String.class);
            keyIndex = this.conf.getInt(PARAMETER_KEY_INDEX, -1);

            if (keyIndex >= columns.size()) {
                log.warn("key index out of range, keyIndex:{}", keyIndex);
                keyIndex = -1;
            }

            Map<String, Object> props = new HashMap<>();
            props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, this.conf.getString(PARAMETER_BOOTSTRAP_SERVERS));

            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.RETRIES_CONFIG, 0);
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
            props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            Map<String, Object> customizeProps = this.conf.getMap(PARAMETER_PROPS, Object.class);
            if (null != customizeProps) {
                props.putAll(customizeProps);
            }

            producer = new KafkaProducer<>(props);

        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            log.info("start to writer kafka.");

            Record record;
            while ((record = lineReceiver.getFromReader()) != null) {
                String key = null;
                if (keyIndex >= 0) {
                    Column column = record.getColumn(keyIndex);
                    Object rawData = column.getRawData();
                    if (null != rawData) {
                        key = JSON.toJSONString(rawData);
                    }
                }

                String value = recordToJson(record);
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                log.debug("send data. key: {}, value: {}", key, value);
                producer.send(producerRecord);
            }
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
            if (producer != null) {
                producer.close();
            }
        }

        private String recordToJson(Record record) {
            final int size = columns.size();
            final Map<String, String> data = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                final Column column = record.getColumn(i);
                data.put(columns.get(i), column.asString());
            }
            return JSON.toJSONString(data, JSONWriter.Feature.WriteMapNullValue);
        }
    }
}
