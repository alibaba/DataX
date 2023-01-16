package com.alibaba.datax.plugin.writer.tdenginewriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum TDengineWriterErrorCode implements ErrorCode {

    REQUIRED_VALUE("TDengineWriter-00", "缺失必要的值"),
    ILLEGAL_VALUE("TDengineWriter-01", "值非法"),
    RUNTIME_EXCEPTION("TDengineWriter-02", "运行时异常"),
    TYPE_ERROR("TDengineWriter-03", "Datax类型无法正确映射到TDengine类型");

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
