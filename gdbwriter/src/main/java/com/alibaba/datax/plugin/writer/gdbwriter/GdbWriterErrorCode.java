package com.alibaba.datax.plugin.writer.gdbwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum GdbWriterErrorCode implements ErrorCode {
    BAD_CONFIG_VALUE("GdbWriter-00", "您配置的值不合法."),
    CONFIG_ITEM_MISS("GdbWriter-01", "您配置项缺失."),
    FAIL_CLIENT_CONNECT("GdbWriter-02", "GDB连接异常."),;

    private final String code;
    private final String description;

    private GdbWriterErrorCode(String code, String description) {
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