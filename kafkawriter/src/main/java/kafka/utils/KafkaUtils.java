package kafka.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import kafka.config.KafkaConfig;
import kafka.producer.BaseKafkaProducer;
import kafka.producer.KafkaProducerManager;


/**
 * @author huypva
 */
public class KafkaUtils {
    private KafkaUtils() {
    }

    private static final Log LOGGER = LogFactory.getLog(KafkaUtils.class);

    public static boolean sendMsg(KafkaConfig config, String msg) {
        try {
            BaseKafkaProducer producer = KafkaProducerManager.getClient(config);
            return producer.sendMessage(msg);
        } catch (Exception ex) {
            LOGGER.error("Send msg to kafka error!" + ex.getMessage(), ex);
            return false;
        }
    }

    public static boolean sendMsg(KafkaConfig config, String topic, String msg) {
        try {
            BaseKafkaProducer producer = KafkaProducerManager.getClient(config);
            return producer.sendMessage(topic, msg);
        } catch (Exception ex) {
            LOGGER.error("Send msg to kafka error!" + ex.getMessage(), ex);
            return false;
        }
    }

}
