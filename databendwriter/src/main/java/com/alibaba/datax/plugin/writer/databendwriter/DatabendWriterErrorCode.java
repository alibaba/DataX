package com.alibaba.datax.plugin.writer.databendwriter;

import com.alibaba.datax.common.spi.ErrorCode;


public enum DatabendWriterErrorCode implements ErrorCode {
    CONF_ERROR("DatabendWriter-00", "配置错误."),
    WRITE_DATA_ERROR("DatabendWriter-01", "写入数据时失败."),
    ;

    private final String code;
    private final String description;

    private DatabendWriterErrorCode(String code, String description) {
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
        return String.format("Code:[%s], Description:[%s].", this.code, this.description);
    }
}