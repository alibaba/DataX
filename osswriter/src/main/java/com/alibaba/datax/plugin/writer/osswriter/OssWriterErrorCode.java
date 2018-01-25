package com.alibaba.datax.plugin.writer.osswriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by haiwei.luo on 14-9-17.
 */
public enum OssWriterErrorCode implements ErrorCode {
    
    CONFIG_INVALID_EXCEPTION("OssWriter-00", "您的参数配置错误."),
    REQUIRED_VALUE("OssWriter-01", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("OssWriter-02", "您填写的参数值不合法."),
    Write_OBJECT_ERROR("OssWriter-03", "您配置的目标Object在写入时异常."),
    OSS_COMM_ERROR("OssWriter-05", "执行相应的OSS操作异常."),
    ;

    private final String code;
    private final String description;

    private OssWriterErrorCode(String code, String description) {
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
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }

}
