/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package kafka.producer;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kafka.config.KafkaConfig;

import java.util.HashMap;
import java.util.Map;

public class KafkaProducerManager {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerManager.class);
    private static final Map<String, KafkaProducerPool> poolMap = new HashMap<>();

    private KafkaProducerManager() {
    }

    private static String createMapKey(KafkaConfig config) {
        return String.format("%s", config.getKeyString());
    }

    public static void initProducer(KafkaConfig config) throws Exception {
        KafkaProducerPool producer = new KafkaProducerPool(config);
        poolMap.put(createMapKey(config), producer);
    }

    public static BaseKafkaProducer getClient(KafkaConfig config) {
        return poolMap.get(createMapKey(config)).getClient();
    }

    public static void shutdownALLConnections() {
        for (KafkaProducerPool pool : poolMap.values()) {
            try {
                pool.shutDownALLConnections();
            } catch (Exception ex) {
                logger.error("Exception when shutdown {}", ex.getMessage());
            }
        }
    }
}
