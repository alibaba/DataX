package com.alibaba.datax.plugin.reader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum TDengineReaderErrorCode implements ErrorCode {

    REQUIRED_VALUE("TDengineReader-00", "缺失必要的值"),
    ILLEGAL_VALUE("TDengineReader-01", "值非法"),
    CONNECTION_FAILED("TDengineReader-02", "连接错误");

    private final String code;
    private final String description;

    TDengineReaderErrorCode(String code, String description) {
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
