package com.alibaba.datax.plugin.writer.kafkawriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Kafka Writer 错误代码枚举类
 *
 * @author LZC
 * @date 2024-04-23
 **/
public enum KafkaWriterError implements ErrorCode {
    SERVERS("500", "Kafka中心bootstrapServers"),
    TOPIC("500", "Kafka topic"),
    KEYS("500", "发送Kafka数据的字段名"),
    ;

    private String code;
    private String desc;

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
