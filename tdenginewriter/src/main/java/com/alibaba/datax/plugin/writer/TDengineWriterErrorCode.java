package com.alibaba.datax.plugin.writer;

import com.alibaba.datax.common.spi.ErrorCode;

public enum TDengineWriterErrorCode implements ErrorCode {
    RUNTIME_EXCEPTION("TDengineWriter-00", "运行时异常");

    private final String code;
    private final String description;

    private TDengineWriterErrorCode(String code, String description) {
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
