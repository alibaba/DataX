package com.alibaba.datax.plugin.writer.tsdbwriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：TSDB Http Writer Error Code
 *
 * @author Benedict Jin
 * @since 2019-04-18
 */
public enum TSDBWriterErrorCode implements ErrorCode {

    REQUIRED_VALUE("TSDBWriter-00", "Missing the necessary value"),
    ILLEGAL_VALUE("TSDBWriter-01", "Illegal value"),
    RUNTIME_EXCEPTION("TSDBWriter-01", "Runtime exception"),
    RETRY_WRITER_EXCEPTION("TSDBWriter-02", "After repeated attempts, the write still fails");

    private final String code;
    private final String description;

    TSDBWriterErrorCode(String code, String description) {
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
