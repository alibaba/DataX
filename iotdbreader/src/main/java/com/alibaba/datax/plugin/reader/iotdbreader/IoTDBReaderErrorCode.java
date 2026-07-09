package com.alibaba.datax.plugin.reader.iotdbreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum IoTDBReaderErrorCode implements ErrorCode {

    REQUIRED_VALUE("IoTDBReader-00", "parameter value is missing"),
    ILLEGAL_VALUE("IoTDBReader-01", "invalid parameter value"),
    CONNECTION_FAILED("IoTDBReader-02", "connection error");

    private final String code;
    private final String description;

    IoTDBReaderErrorCode(String code, String description) {
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
        return String.format("Code:[%s], Description:[%s].", this.code, this.description);
    }
}
