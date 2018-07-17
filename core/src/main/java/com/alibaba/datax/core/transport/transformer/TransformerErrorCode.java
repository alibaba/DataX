package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.spi.ErrorCode;

public enum TransformerErrorCode implements ErrorCode {
    //重复命名
    TRANSFORMER_NAME_ERROR("TransformerErrorCode-01","Transformer name illegal"),
    TRANSFORMER_DUPLICATE_ERROR("TransformerErrorCode-02","Transformer name has existed"),
    TRANSFORMER_NOTFOUND_ERROR("TransformerErrorCode-03","Transformer name not found"),
    TRANSFORMER_CONFIGURATION_ERROR("TransformerErrorCode-04","Transformer configuration error"),
    TRANSFORMER_ILLEGAL_PARAMETER("TransformerErrorCode-05","Transformer parameter illegal"),
    TRANSFORMER_RUN_EXCEPTION("TransformerErrorCode-06","Transformer run exception"),
    TRANSFORMER_GROOVY_INIT_EXCEPTION("TransformerErrorCode-07","Transformer Groovy init exception"),
    ;

    private final String code;

    private final String description;

    private TransformerErrorCode(String code, String description) {
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
