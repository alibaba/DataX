package com.leehom.arch.datax.plugin.rdb2graph.writer.neo4jwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum Neo4jWriterErrorCode implements ErrorCode {
    BAD_CONFIG_VALUE("Neo4jWriter-00", "配置的值不合法."),
    ERROR_LOAD_RDBSCHEMA("Neo4jWriter-01", "载入关系模式异常."),
    UNSUPPORTED_TYPE("Neo4jWriter-02", "不支持字段类型."),
    WRONG_RECORD_TYPE("Neo4jWriter-03, {}, {}", "错误记录类型."),
    WRONG_NEO4j_CLIENT("Neo4jWriter-04", "neo4j client访问异常."),
    ;

    private final String code;
    private final String description;

    Neo4jWriterErrorCode(String code, String description) {
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