package com.alibaba.datax.plugin.writer.mongodbwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum MongoDBWriterErrorCode implements ErrorCode {

    ILLEGAL_VALUE("ILLEGAL_PARAMETER_VALUE","参数不合法"),
    ILLEGAL_ADDRESS("ILLEGAL_ADDRESS","不合法的Mongo地址"),
    JSONCAST_EXCEPTION("JSONCAST_EXCEPTION","json类型转换异常"),
    UNEXCEPT_EXCEPTION("UNEXCEPT_EXCEPTION","未知异常");

    private final String code;

    private final String description;

    private MongoDBWriterErrorCode(String code,String description) {
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
