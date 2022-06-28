package com.alibaba.datax.plugin.writer.tdenginewriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum TDengineWriterErrorCode implements ErrorCode {

    REQUIRED_VALUE("TDengineWriter-00", "parameter value is missing"),
    ILLEGAL_VALUE("TDengineWriter-01", "invalid parameter value"),
    RUNTIME_EXCEPTION("TDengineWriter-02", "runtime exception"),
    TYPE_ERROR("TDengineWriter-03", "data type mapping error");

    private final String code;
    private final String description;

    TDengineWriterErrorCode(String code, String description) {
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
