package com.alibaba.datax.plugin;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by jianying.wcj on 2015/3/19 0019.
 */
public enum ReaderErrorCode implements ErrorCode {

    UNSUPPORTED_TYPE("UNSUPPORTED_TYPE", "参数不合法"),
    SQL_EXECUTION_ERROR("SQL_EXECUTION_ERROR", "sql执行出错");

    private final String code;

    private final String description;

    private ReaderErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}

