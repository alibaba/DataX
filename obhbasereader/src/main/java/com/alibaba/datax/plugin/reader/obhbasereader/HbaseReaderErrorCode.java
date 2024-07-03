package com.alibaba.datax.plugin.reader.obhbasereader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum HbaseReaderErrorCode implements ErrorCode {
    REQUIRED_VALUE("HbaseReader-00", "Missing required parameters."),
    ILLEGAL_VALUE("HbaseReader-01", "Illegal configuration."),
    PREPAR_READ_ERROR("HbaseReader-02", "Preparing to read HBase error."),
    SPLIT_ERROR("HbaseReader-03", "Splitting HBase table error."),
    INIT_TABLE_ERROR("HbaseReader-04", "Initializing HBase extraction table error"),
    PARSE_COLUMN_ERROR("HbaseReader-05", "Parse column failed.");

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
