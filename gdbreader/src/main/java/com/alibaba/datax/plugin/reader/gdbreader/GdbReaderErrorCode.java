package com.alibaba.datax.plugin.reader.gdbreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum GdbReaderErrorCode implements ErrorCode {
    /**
     *
     */
    BAD_CONFIG_VALUE("GdbReader-00", "The value you configured is invalid."),
    FAIL_CLIENT_CONNECT("GdbReader-02", "GDB connection is abnormal."),
    UNSUPPORTED_TYPE("GdbReader-03", "Unsupported data type conversion."),
    FAIL_FETCH_LABELS("GdbReader-04", "Error pulling all labels, it is recommended to configure the specified label pull."),
    FAIL_FETCH_IDS("GdbReader-05", "Pull range id error."),
    ;

    private final String code;
    private final String description;

    private GdbReaderErrorCode(String code, String description) {
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