package com.alibaba.datax.plugin.reader.jsonfilereader;
import com.alibaba.datax.common.spi.ErrorCode;


public enum JsonReaderErrorCode implements ErrorCode{
    REQUIRED_VALUE("JsonFilereader-00", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("JsonFilereader-01", "您填写的参数值不合法."),
    MIXED_INDEX_VALUE("JsonFilereader-02", "您的列信息配置同时包含了index,value."),
    NO_INDEX_VALUE("JsonFilereader-03","您明确的配置列信息,但未填写相应的index,value."),
    FILE_NOT_EXISTS("JsonFilereader-04", "您配置的目录文件路径不存在."),
    OPEN_FILE_WITH_CHARSET_ERROR("JsonFilereader-05", "您配置的文件编码和实际文件编码不符合."),
    OPEN_FILE_ERROR("JsonFilereader-06", "您配置的文件在打开时异常,建议您检查源目录是否有隐藏文件,管道文件等特殊文件."),
    READ_FILE_IO_ERROR("JsonFilereader-07", "您配置的文件在读取时出现IO异常."),
    SECURITY_NOT_ENOUGH("JsonFilereader-08", "您缺少权限执行相应的文件操作."),
    CONFIG_INVALID_EXCEPTION("JsonFilereader-09", "您的参数配置错误."),
    NOT_SUPPORT_TYPE("JsonFilereaderr-10","您配置的列类型暂不支持."),
    EMPTY_DIR_EXCEPTION("JsonFilereader-11", "您尝试读取的文件目录为空."),;

    private final String code;
    private final String description;

    private JsonReaderErrorCode(String code, String description) {
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



