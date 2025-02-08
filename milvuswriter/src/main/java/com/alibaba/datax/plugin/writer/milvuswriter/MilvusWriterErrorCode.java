package com.alibaba.datax.plugin.writer.milvuswriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @author ziming(子茗)
 * @date 12/27/24
 * @description
 */
public enum MilvusWriterErrorCode implements ErrorCode {
    MILVUS_COLLECTION("MilvusWriter-01", "collection process error"),
    REQUIRED_VALUE("MilvusWriter-02", "miss required parameter");
    private final String code;
    private final String description;

    MilvusWriterErrorCode(String code, String description) {
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
