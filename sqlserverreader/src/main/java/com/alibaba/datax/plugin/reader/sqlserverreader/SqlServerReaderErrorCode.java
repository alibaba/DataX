package com.alibaba.datax.plugin.reader.sqlserverreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum SqlServerReaderErrorCode implements ErrorCode {
    ;

    private String code;
    private String description;

    private SqlServerReaderErrorCode(String code, String description) {
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

}
