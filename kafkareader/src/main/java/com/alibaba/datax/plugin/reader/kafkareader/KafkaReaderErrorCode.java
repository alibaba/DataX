package com.alibaba.datax.plugin.reader.kafkareader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum KafkaReaderErrorCode implements ErrorCode {
    TOPIC_ERROR("KafkaReader-01", "没有设置参数[topic]"),
    PARTITION_ERROR("KafkaReader-02", "没有设置参数[kafka.partitions]."),
    GET_TOPOLOGY_FAILED("KafkaReader-03", "获取 drds 表的拓扑结构失败."),
    ADDRESS_ERROR("KafkaReader-04", "没有设置参数[bootstrap.servers]."),
    KAFKA_READER_ERROR("KafkaReader-05", "没有设置参数[groupid]."),
    COLUMN_ERROR("KafkaReader-05","JSON列信息有误"),
    READ_TYPE_ERROR("KafkaReader-06","kafka读取方式有误")
    ;

    private final String code;
    private final String description;

    private KafkaReaderErrorCode(String code, String description) {
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
