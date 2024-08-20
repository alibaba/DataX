package com.alibaba.datax.plugin.writer.kafkawriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @author tanghaiyang
 */
public enum KafkaWriterError implements ErrorCode {
    KAFKA_CONN_BOOTSTRAP_SERVERS_MISSING("KAFKA_CONN_BOOTSTRAP_SERVERS_MISSING", "Kafka broker地址错误，请检查填写的broker地址是否正确"),
    KAFKA_CONN_TOPIC_MISSING("KAFKA_CONN_TOPIC_MISSING", "请检查Kafka topic配置是否正确"),
    KAFKA_COLUMN_MISSING("KAFKA_COLUMN_MISSING", "请检查Kafka字段配置是否正确"),
    ;

    private final String code;
    private final String desc;

    KafkaWriterError(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.desc;
    }
}
