package com.alibaba.datax.plugin.reader.streamreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum StreamReaderErrorCode implements ErrorCode {
    REQUIRED_VALUE("StreamReader-00", "缺失必要的值"),
    ILLEGAL_VALUE("StreamReader-01", "值非法"),
    NOT_SUPPORT_TYPE("StreamReader-02", "不支持的column类型"),;


    private final String code;
    private final String description;

    private StreamReaderErrorCode(String code, String description) {
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
