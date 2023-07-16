package com.alibaba.datax.plugin.reader.elasticsearchreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum ESReaderErrorCode implements ErrorCode {

    ES_CONNECT_ERROR("ESreader-01", "ES连接异常."),
    ;

    private final String code;
    private final String description;

    ESReaderErrorCode(String code, String description) {
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