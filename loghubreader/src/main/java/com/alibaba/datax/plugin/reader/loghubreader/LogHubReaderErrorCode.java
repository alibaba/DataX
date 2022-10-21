package com.alibaba.datax.plugin.reader.loghubreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum LogHubReaderErrorCode implements ErrorCode {
    BAD_CONFIG_VALUE("LogHuReader-00", "The value you configured is invalid."),
    LOG_HUB_ERROR("LogHubReader-01","LogHub access encounter exception"),
    REQUIRE_VALUE("LogHubReader-02","Missing parameters"),
    EMPTY_LOGSTORE_VALUE("LogHubReader-03","There is no shard in this LogStore");
    
    private final String code;
    private final String description;

    private LogHubReaderErrorCode(String code, String description) {
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
