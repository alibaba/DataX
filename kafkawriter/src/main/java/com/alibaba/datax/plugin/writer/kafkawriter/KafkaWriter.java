package com.alibaba.datax.plugin.writer.kafkawriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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

/**
 * Kafka Writer
 *
 * @author LZC
 * @version 0.0.1
 * @date 2024-04-23
 **/
public class KafkaWriter extends Writer {

    public static final String PARAMETER_BOOTSTRAP_SERVERS = "bootstrapServers";
    public static final String PARAMETER_TOPIC = "topic";
    public static final String PARAMETER_KEYS = "keys";

    /**
     * Job实现
     *
     * @author LZC
     * @version 0.0.1
     * @date 2024-04-23
     **/
    public static class Job extends Writer.Job {
        private static final Logger log = LoggerFactory.getLogger(Job.class);

        private Configuration conf = null;

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
            log.debug("kafka writer job conf:{}", this.conf.toJSON());
            this.conf.getNecessaryValue(PARAMETER_BOOTSTRAP_SERVERS, KafkaWriterError.SERVERS);
            this.conf.getNecessaryValue(PARAMETER_TOPIC, KafkaWriterError.TOPIC);
            this.conf.getNecessaryValue(PARAMETER_KEYS, KafkaWriterError.KEYS);
        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configList = new ArrayList<Configuration>();
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

    /**
     * Task实现
     *
     * @author LZC
     * @version 0.0.1
     * @date 2024-04-23
     **/
    public static class Task extends Writer.Task {
        private static final Logger log = LoggerFactory.getLogger(Task.class);

        private Configuration conf = null;
        private Producer<String, String> producer;

        private List<String> keys = Lists.newArrayList();

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
            keys = this.conf.getList(PARAMETER_KEYS, String.class);

            Map<String, Object> props = new HashMap<>();
            props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, this.conf.getString(PARAMETER_BOOTSTRAP_SERVERS));

            // 这意味着leader需要等待所有备份都成功写入日志，这种策略会保证只要有一个备份存活就不会丢失数据。这是最强的保证。
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.RETRIES_CONFIG, 0);
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
            props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            producer = new KafkaProducer<>(props);

        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            log.info("===start to writer kafka===");

            Record record;
            while ((record = lineReceiver.getFromReader()) != null) {
                final String s = recordToJson(record);
                // 说明还在读取数据,或者读取的数据没处理完，获取一行数据，按照指定分隔符 拼成字符串 发送出去
                final ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(this.conf.getString(PARAMETER_TOPIC), s);
                log.debug("===data==={}", s);
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

        /**
         * 将Record中的数据按字段属性顺序转换成json格式的字符串数据
         *
         * @param record
         * @return java.lang.String
         * @throws
         * @author LZC
         * @version
         * @date 2024-04-23
         **/
        private String recordToJson(Record record) {
            final HashMap<String, String> map = Maps.newHashMap();
            final int size = keys.size();
            for (int i = 0; i < size; i++) {
                final Column column = record.getColumn(i);
                map.put(keys.get(i), column.asString());
            }
            return JSON.toJSONString(map, JSONWriter.Feature.WriteMapNullValue);
        }
    }
}
