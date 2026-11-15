package kafka.config;


public class KafkaConfig {

    public String brokers;
    public String topic;

    public String groupID = "default";
    public int noComsumer = 8;
    public int noProducerPoolSize = 1;

    public int sessionTimoutMs = 30000;
    public int commitIntervalMs = 30000;

    //Added by BangDQ   
    public String acks = "all";//acks=0 not wait, acks=1 , acks=-1 or all
    public String key_serializer = "org.apache.kafka.common.serialization.StringSerializer";//string key
    public String value_serializer = "org.apache.kafka.common.serialization.StringSerializer";//string value
    public long buffer_memory = 33554432;
    public int retries = 0;
    public int batch_size = 200;//0 : disable this function, default = 16384
    public long linger_ms = 0; //0:no delay util batch.size got    
    //for consumer
    public int max_poll_records = 100; //default 500

    public String dnsLookup = DNSLookupMode.USE_ALL_DNS_IPS;

    public String getKeyString() {
        return String.format("%s_%s_%s", brokers, groupID, topic);
    }

    @Override
    public String toString() {
        return "KafkaConfig{" +
                "brokers='" + brokers + '\'' +
                ", topic='" + topic + '\'' +
                ", groupID='" + groupID + '\'' +
                ", noComsumer=" + noComsumer +
                ", noProducerPoolSize=" + noProducerPoolSize +
                ", sessionTimoutMs=" + sessionTimoutMs +
                ", commitIntervalMs=" + commitIntervalMs +
                ", acks='" + acks + '\'' +
                ", key_serializer='" + key_serializer + '\'' +
                ", value_serializer='" + value_serializer + '\'' +
                ", buffer_memory=" + buffer_memory +
                ", retries=" + retries +
                ", batch_size=" + batch_size +
                ", linger_ms=" + linger_ms +
                ", max_poll_records=" + max_poll_records +
                ", dnsLookup='" + dnsLookup + '\'' +
                '}';
    }
}
