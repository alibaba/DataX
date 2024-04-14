package com.alibaba.datax.plugin.writer.neo4jwriter.exception;

import com.alibaba.datax.common.spi.ErrorCode;


public enum Neo4jErrorCode implements ErrorCode {

    /**
     * Invalid configuration
     * 配置校验异常
     */
    CONFIG_INVALID("NEO4J_ERROR_01","invalid configuration"),
    /**
     * database error
     * 在执行写入到数据库时抛出的异常，可能是权限异常，也可能是连接超时，或者是配置到了从节点。
     * 如果是更新操作，还会有死锁异常。具体原因根据报错信息确定，但是这与dataX无关。
     */
    DATABASE_ERROR("NEO4J_ERROR_02","database error");

    private final String code;
    private final String description;

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }

    Neo4jErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }
}
