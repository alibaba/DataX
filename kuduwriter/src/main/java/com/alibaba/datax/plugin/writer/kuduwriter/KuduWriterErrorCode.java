package com.alibaba.datax.plugin.writer.kuduwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum KuduWriterErrorCode implements ErrorCode {

    ILLEGAL_VALUE("ILLEGAL_PARAMETER_VALUE","参数不合法"),
    ILLEGAL_ADDRESS("ILLEGAL_ADDRESS","不合法的Kudu地址"),
    CLOSE_EXCEPTION("UNEXCEPT_EXCEPTION","关闭异常"),
    UNKNOWN_TYPE("UNKNOWN_TYPE","未知类型");

    private final String code;

    private final String description;

    private KuduWriterErrorCode(String code,String description) {
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
