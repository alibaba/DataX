package com.alibaba.datax.plugin.reader.iotdbreader;

import com.alibaba.datax.common.spi.ErrorCode;

public class IoTDBReaderErrorCode implements ErrorCode {
    private final String code;
    private final String description;

    public IoTDBReaderErrorCode(String code, String description) {
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
