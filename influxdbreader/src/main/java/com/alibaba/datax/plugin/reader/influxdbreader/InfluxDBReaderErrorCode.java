package com.alibaba.datax.plugin.reader.influxdbreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum InfluxDBReaderErrorCode implements ErrorCode {
    // Reqiored Value
    REQUIRED_VALUE("InfluxDBReader-00", "parameter value is missing"),
    ILLEGAL_VALUE("InfluxDBReader-01", "invalid parameter value");

    private final String code;
    private final String description;

    InfluxDBReaderErrorCode(String code, String description) {
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
        return String.format("Code:[%s], Description:[%s]. ", this.code, this.description);
    }
}
