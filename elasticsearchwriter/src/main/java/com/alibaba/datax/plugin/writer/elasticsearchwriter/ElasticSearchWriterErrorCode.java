package com.alibaba.datax.plugin.writer.elasticsearchwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum ElasticSearchWriterErrorCode implements ErrorCode {
    BAD_CONFIG_VALUE("ESWriter-00", "The value you configured is not valid."),
    ES_INDEX_DELETE("ESWriter-01", "Delete index error."),
    ES_INDEX_CREATE("ESWriter-02", "Index creation error."),
    ES_MAPPINGS("ESWriter-03", "The mappings error."),
    ES_INDEX_INSERT("ESWriter-04", "Insert data error."),
    ES_ALIAS_MODIFY("ESWriter-05", "Alias modification error."),
    JSON_PARSE("ESWrite-06", "Json format parsing error"),
    UPDATE_WITH_ID("ESWrite-07", "Update mode must specify column type with id"),
    RECORD_FIELD_NOT_FOUND("ESWrite-08", "Field does not exist in the original table"),
    ES_GET_SETTINGS("ESWriter-09", "get settings failed");
    ;

    private final String code;
    private final String description;

    ElasticSearchWriterErrorCode(String code, String description) {
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
