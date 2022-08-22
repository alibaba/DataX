package com.alibaba.datax.plugin.reader.elasticsearchreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum ESReaderErrorCode implements ErrorCode {
    BAD_CONFIG_VALUE("ESReader-00", "您配置的值不合法."),
    ES_SEARCH_ERROR("ESReader-01", "search出错."),
    ES_INDEX_NOT_EXISTS("ESReader-02", "index不存在."),
    UNKNOWN_DATA_TYPE("ESReader-03", "无法识别的数据类型."),
    COLUMN_CANT_BE_EMPTY("ESReader-04", "column不能为空."),
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