package com.alibaba.datax.plugin.reader.obsreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum ObsReaderErrorCode implements ErrorCode {
    // TODO: 修改错误码类型
    RUNTIME_EXCEPTION("ObsReader-00", "运行时异常"),
    OBS_EXCEPTION("ObsFileReader-01", "Obs配置异常"),
    CONFIG_INVALID_EXCEPTION("ObsFileReader-02", "参数配置错误"),
    NOT_SUPPORT_TYPE("ObsReader-03", "不支持的类型"),
    CAST_VALUE_TYPE_ERROR("ObsFileReader-04", "无法完成指定类型的转换"),
    SECURITY_EXCEPTION("ObsReader-05", "缺少权限"),
    ILLEGAL_VALUE("ObsReader-06", "值错误"),
    REQUIRED_VALUE("ObsReader-07", "必选项"),
    NO_INDEX_VALUE("ObsReader-08","没有 Index" ),
    MIXED_INDEX_VALUE("ObsReader-09","index 和 value 混合" ),
    EMPTY_BUCKET_EXCEPTION("ObsReader-10", "您尝试读取的Bucket为空");

    private final String code;
    private final String description;

    private ObsReaderErrorCode(String code, String description) {
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