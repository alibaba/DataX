package com.alibaba.datax.plugin.writer.kafkawriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson2.JSONObject;
import kafka.config.KafkaConfig;
import kafka.producer.BaseKafkaProducer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class KafkaWriter extends Writer {
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private Configuration originalConfig;


        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            String topic = this.originalConfig.getString(Key.TOPIC, null);
            String brokers = this.originalConfig.getString(Key.BROKERS, null);
            validateKafkaConfig(topic, brokers);
        }

        private void validateKafkaConfig(String topic, String brokers) {
            if (!StringUtils.isNoneBlank(topic)) {
                throw DataXException.asDataXException(
                        KafkaWriterErrorCode.ILLEGAL_VALUE,
                        String.format("Config topic is invalid: [%s]", topic)
                );
            }
            if (!StringUtils.isNoneBlank(brokers)) {
                throw DataXException.asDataXException(
                        KafkaWriterErrorCode.ILLEGAL_VALUE,
                        String.format("Config brokers is invalid: [%s]", brokers)
                );
            }
        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                writerSplitConfigs.add(this.originalConfig);
            }

            return writerSplitConfigs;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);

        private Configuration writerSliceConfig;

        private String topic;

        private String brokers;

        private boolean isSilence;

        private List<String> column;

        private BaseKafkaProducer producer;

        @Override
        public void init() {
            this.writerSliceConfig = getPluginJobConf();

            this.topic = this.writerSliceConfig.getString(Key.TOPIC, null);
            this.brokers = this.writerSliceConfig.getString(Key.BROKERS, null);
            this.isSilence = this.writerSliceConfig.getBool(Key.IS_SILENCE, false);

            this.column = this.writerSliceConfig.getList(Key.COLUMN, String.class);

            KafkaConfig kafkaConfig = new KafkaConfig();
            kafkaConfig.brokers = brokers;
            kafkaConfig.topic = topic;
            this.producer = new BaseKafkaProducer(kafkaConfig);
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            try {
                Record r;
                while ((r = recordReceiver.getFromReader()) != null) {
                    JSONObject msg = new JSONObject();
                    for (int i = 0; i < r.getColumnNumber(); i++) {
                        switch (r.getColumn(i).getType()){
                            case LONG:
                                msg.put(this.column.get(i), r.getColumn(i).asLong());
                                break;
                            case BOOL:
                                msg.put(this.column.get(i), r.getColumn(i).asBoolean());
                                break;
                            case DATE:
                                msg.put(this.column.get(i), r.getColumn(i).asDate());
                                break;
                            case DOUBLE:
                                msg.put(this.column.get(i), r.getColumn(i).asDouble());
                                break;
                            case BYTES:
                                msg.put(this.column.get(i), r.getColumn(i).asBytes());
                                break;
                            case STRING:
                                msg.put(this.column.get(i), r.getColumn(i).asString());
                                break;
                            default:
                                throw new Exception(String.format("Invalid column type [%s]: %s",
                                        this.column.get(i),r.getColumn(i).getType()));
                        }
                    }

                    String raw = msg.toJSONString();
                    if (!isSilence) {
                        LOG.info("start write kafka :[{}]. context info:{}.", this.topic, raw);
                    }
                    this.producer.sendMessage(raw);
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(KafkaWriterErrorCode.RUNTIME_EXCEPTION, e);
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
