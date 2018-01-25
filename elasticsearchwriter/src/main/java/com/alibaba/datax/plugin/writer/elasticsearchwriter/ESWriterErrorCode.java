package com.alibaba.datax.plugin.writer.elasticsearchwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum ESWriterErrorCode implements ErrorCode {
    BAD_CONFIG_VALUE("ESWriter-00", "您配置的值不合法."),
    ES_INDEX_DELETE("ESWriter-01", "删除index错误."),
    ES_INDEX_CREATE("ESWriter-02", "创建index错误."),
    ES_MAPPINGS("ESWriter-03", "mappings错误."),
    ES_INDEX_INSERT("ESWriter-04", "插入数据错误."),
    ES_ALIAS_MODIFY("ESWriter-05", "别名修改错误."),
    ;

    private final String code;
    private final String description;

    ESWriterErrorCode(String code, String description) {
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