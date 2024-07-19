package com.alibaba.datax.plugin.reader.obhbasereader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum HbaseReaderErrorCode implements ErrorCode {
    REQUIRED_VALUE("ObHbaseReader-00", "Missing required parameters."),
    ILLEGAL_VALUE("ObHbaseReader-01", "Illegal configuration."),
    PREPAR_READ_ERROR("ObHbaseReader-02", "Preparing to read ObHBase error."),
    SPLIT_ERROR("ObHbaseReader-03", "Splitting ObHBase table error."),
    INIT_TABLE_ERROR("ObHbaseReader-04", "Initializing ObHBase extraction table error"),
    PARSE_COLUMN_ERROR("ObHbaseReader-05", "Parse column failed."),
    READ_ERROR("ObHbaseReader-06", "Read ObHBase error.");

    private final String code;
    private final String description;

    private HbaseReaderErrorCode(String code, String description) {
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
