package com.alibaba.datax.plugin.reader.s3reader;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @author mengxin.liumx
 * @author L.cm
 */
public enum S3ReaderErrorCode implements ErrorCode {
    /**
     * TODO: 修改错误码类型
     */
    RUNTIME_EXCEPTION("S3Reader-00", "运行时异常"),
    S3_EXCEPTION("S3FileReader-01", "S3配置异常"),
    CONFIG_INVALID_EXCEPTION("S3FileReader-02", "参数配置错误"),
    NOT_SUPPORT_TYPE("S3Reader-03", "不支持的类型"),
    CAST_VALUE_TYPE_ERROR("S3FileReader-04", "无法完成指定类型的转换"),
    SECURITY_EXCEPTION("S3Reader-05", "缺少权限"),
    ILLEGAL_VALUE("S3Reader-06", "值错误"),
    REQUIRED_VALUE("S3Reader-07", "必选项"),
    NO_INDEX_VALUE("S3Reader-08", "没有 Index"),
    MIXED_INDEX_VALUE("S3Reader-09", "index 和 value 混合"),
    EMPTY_BUCKET_EXCEPTION("S3Reader-10", "您尝试读取的Bucket为空");

    private final String code;
    private final String description;

    S3ReaderErrorCode(String code, String description) {
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
