package com.alibaba.datax.plugin.reader.elasticsearchreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum ESReaderErrorCode implements ErrorCode {
    BAD_CONFIG_VALUE("ESWriter-00", "您配置的值不合法."),
    QUERY_FAILED("queryFailed","查询失败"),
    UNKNOWN_DATA_TYPE("unknownDataType","未知的数据类型"),
    ;



    private final String code;
    private final String description;

    ESReaderErrorCode(String code, String description) {
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