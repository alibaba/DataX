package com.alibaba.datax.example.util;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * TODO: 根据现有日志数据分析各类错误，进行细化。
 * 
 * <p>请不要格式化本类代码</p>
 */
public enum TransformerErrorCode implements ErrorCode {

	TRANSFORMER_INIT_ERROR("Transformer-00", "DataX Transformer 注册失败, 请联系您的运维解决 .");


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
