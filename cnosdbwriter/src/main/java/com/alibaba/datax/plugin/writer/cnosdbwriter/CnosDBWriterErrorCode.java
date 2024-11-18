package com.alibaba.datax.plugin.writer.cnosdbwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum CnosDBWriterErrorCode implements ErrorCode {
    EncodeWriteRequest("CNOSDB_W_11", "failed encode the write request"),
    SendWriteRequestHTTP("CNOSDB_W_21", "failed to do write HTTP request"),
    ParseJSONOpenTSDB("CNOSDB_W_1001", "failed to decode OpenTSDB JSON point"),
    ;

    private final String code;
    private final String description;

    CnosDBWriterErrorCode(String code, String description) {
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
