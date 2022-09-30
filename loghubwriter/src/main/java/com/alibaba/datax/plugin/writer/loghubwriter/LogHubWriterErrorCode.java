package com.alibaba.datax.plugin.writer.loghubwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum LogHubWriterErrorCode implements ErrorCode {
    BAD_CONFIG_VALUE("LogHubWriter-00", "The value you configured is invalid."),
    LOG_HUB_ERROR("LogHubWriter-01","LogHub access encounter exception"),
    REQUIRE_VALUE("LogHubWriter-02","Missing parameters");

    private final String code;
    private final String description;

    private LogHubWriterErrorCode(String code, String description) {
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