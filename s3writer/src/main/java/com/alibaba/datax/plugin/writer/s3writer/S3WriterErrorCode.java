package com.alibaba.datax.plugin.writer.s3writer;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @author mengxin.liumx
 * @author L.cm
 */
public enum S3WriterErrorCode implements ErrorCode {

    /**
     * 异常 code
     */
    CONFIG_INVALID_EXCEPTION("S3Writer-00", "您的参数配置错误."),
    REQUIRED_VALUE("S3Writer-01", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("S3Writer-02", "您填写的参数值不合法."),
    Write_OBJECT_ERROR("S3Writer-03", "您配置的目标Object在写入时异常."),
    OSS_COMM_ERROR("S3Writer-05", "执行相应的S3操作异常."),
    ;

    private final String code;
    private final String description;

    S3WriterErrorCode(String code, String description) {
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
        return String.format("Code:[%s], Description:[%s].", this.code, this.description);
    }

}
