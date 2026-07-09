package com.alibaba.datax.plugin.writer.iotdbwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum IoTDBWriterErrorCode implements ErrorCode {

    REQUIRED_VALUE("IoTDBWriter-00", "parameter value is missing");

    private final String code;
    private final String description;

    IoTDBWriterErrorCode(String code, String description) {
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
