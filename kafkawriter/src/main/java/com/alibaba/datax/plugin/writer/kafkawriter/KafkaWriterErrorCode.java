package com.alibaba.datax.plugin.writer.kafkawriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum KafkaWriterErrorCode implements ErrorCode {
    RUNTIME_EXCEPTION("KafkaWriter-00", "Runtime exception"),
    ILLEGAL_VALUE("KafkaWriter-01", "Config value invalid."),
    CONFIG_INVALID_EXCEPTION("KafkaWriter-02", "Config invalid."),
    NETWORK_ERROR_KAFKA_WRITER("NetworkKafkaWriter-03", "Can not connect to Kafka Producer");



    private final String code;
    private final String description;

    private KafkaWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
