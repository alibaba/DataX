package com.alibaba.datax.plugin.writer.influxdb2writer;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @author changl
 * @version 1.0
 * @date 2025/6/3
 */
public enum Influxdb2WriterErrorCode implements ErrorCode {

    REQUIRED_VALUE("Influxdb2Writer-00", "Missing the necessary value"),
    RUNTIME_EXCEPTION("Influxdb2Writer-01", "Runtime exception");

    private final String code;
    private final String description;

    Influxdb2WriterErrorCode(String code, String description) {
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
