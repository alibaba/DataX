package com.alibaba.datax.plugin.reader.ossreader;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by mengxin.liumx on 2014/12/7.
 */
public enum OssReaderErrorCode implements ErrorCode {
    // TODO: 修改错误码类型
    RUNTIME_EXCEPTION("OssReader-00", "运行时异常"), 
    OSS_EXCEPTION("OssFileReader-01", "OSS配置异常"),
    CONFIG_INVALID_EXCEPTION("OssFileReader-02", "参数配置错误"), 
    NOT_SUPPORT_TYPE("OssReader-03", "不支持的类型"), 
    CAST_VALUE_TYPE_ERROR("OssFileReader-04", "无法完成指定类型的转换"), 
    SECURITY_EXCEPTION("OssReader-05", "缺少权限"),
    ILLEGAL_VALUE("OssReader-06", "值错误"), 
    REQUIRED_VALUE("OssReader-07", "必选项"), 
    NO_INDEX_VALUE("OssReader-08","没有 Index" ), 
    MIXED_INDEX_VALUE("OssReader-09","index 和 value 混合" ),
    EMPTY_BUCKET_EXCEPTION("OssReader-10", "您尝试读取的Bucket为空");

    private final String code;
    private final String description;

    private OssReaderErrorCode(String code, String description) {
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