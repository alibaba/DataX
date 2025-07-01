/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package kafka.producer;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import kafka.config.KafkaConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaProducerPool {
    private static final Log LOGGER = LogFactory.getLog(KafkaProducerPool.class);

    private final Map<String, BaseKafkaProducer> kafkaClientMap = new HashMap<>();
    private final AtomicInteger usingIndex;
    private final KafkaConfig kafkaConfig;

    private String createMapKey(KafkaConfig kafkaConfig, int pos) {
        return String.format("%s_%d", kafkaConfig.getKeyString(), pos);
    }

    public KafkaProducerPool(KafkaConfig config) {
        kafkaConfig = config;
        usingIndex = new AtomicInteger(0);
        for (int i = 0; i < kafkaConfig.noProducerPoolSize; i++) {
            BaseKafkaProducer custom = new BaseKafkaProducer(kafkaConfig);
            kafkaClientMap.put(createMapKey(kafkaConfig, i), custom);
        }
    }

    public BaseKafkaProducer getClient() {
        int index = usingIndex.incrementAndGet() % kafkaConfig.noProducerPoolSize;
        if (index >= 1000) {
            usingIndex.set(0);
        }

        return kafkaClientMap.get(createMapKey(kafkaConfig, index));
    }

    public void shutDownALLConnections() {
        for (BaseKafkaProducer producer : kafkaClientMap.values()) {
            try {
                producer.close();
            } catch (Exception ex) {
                LOGGER.error(String.format("Exception when shutdown grpcClient  %s  =====>", kafkaConfig.getKeyString()), ex);
            }
        }
    }
}
