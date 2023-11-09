package com.alibaba.datax.plugin.writer.dolphindbwriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * dolphindb error code
 *
 * Created by DolphinDB on 2020/10/15.
 */
public enum DolphinDbWriterErrorCode implements ErrorCode{

    REQUIRED_VALUE("DolphinDbWriter-00", "required parameters missing")
    ;

    private final String code;
    private final String description;

    DolphinDbWriterErrorCode(String code, String description) {
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
