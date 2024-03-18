/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package kafka.producer;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import kafka.config.KafkaConfig;

import java.util.Properties;

public class BaseKafkaProducer {

    private static final Log LOGGER = LogFactory.getLog(BaseKafkaProducer.class);

    private KafkaProducer<String, String> producer;
    private KafkaConfig config;

    public BaseKafkaProducer(KafkaConfig config) {
        try {
            this.config = config;

            Properties prop = createProducerConfig(config);
            producer = new KafkaProducer<>(prop);
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
        }
    }

    public boolean sendMessage(String msg) {
        return sendMessage(config.topic, msg);
    }

    public boolean sendMessage(String topic, String msg) {
        return sendMessage(new ProducerRecord<>(topic, msg));
    }

    public boolean sendMessage(ProducerRecord<String, String> pRecord) {
        try {
            producer.send(pRecord);
            return true;
        } catch (Exception ex) {
            LOGGER.error("KafkaProducer send error!", ex);
            return false;
        }
    }

    private static Properties createProducerConfig(KafkaConfig config) {
        Properties props = new Properties();
        props.put("bootstrap.servers", config.brokers);
        props.put("acks", config.acks);
        props.put("retries", config.retries);
        props.put("batch.size", config.batch_size);
        props.put("linger.ms", config.linger_ms);
        props.put("buffer.memory", config.buffer_memory);
        props.put("key.serializer", config.key_serializer);
        props.put("value.serializer", config.value_serializer);
        props.put("client.dns.lookup", config.dnsLookup);
        return props;
    }

    public void close() {
        try {
            if (producer != null) {
                producer.flush();
                producer.close();
            }
        } catch (Exception ex) {
            LOGGER.error("Close producer error!", ex);
        }
    }
}
