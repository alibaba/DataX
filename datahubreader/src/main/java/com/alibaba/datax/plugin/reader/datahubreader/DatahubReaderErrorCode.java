package com.alibaba.datax.plugin.reader.datahubreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum DatahubReaderErrorCode implements ErrorCode {
    BAD_CONFIG_VALUE("DatahubReader-00", "The value you configured is invalid."),
    LOG_HUB_ERROR("DatahubReader-01","Datahub exception"),
    REQUIRE_VALUE("DatahubReader-02","Missing parameters"),
    EMPTY_LOGSTORE_VALUE("DatahubReader-03","There is no shard under this LogStore");


    private final String code;
    private final String description;

    private DatahubReaderErrorCode(String code, String description) {
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
