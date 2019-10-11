package com.alibaba.datax.plugin.reader.cassandrareader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum CassandraReaderErrorCode implements ErrorCode {
    CONF_ERROR("CassandraReader-00", "配置错误."),
    ;

    private final String code;
    private final String description;

    private CassandraReaderErrorCode(String code, String description) {
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
