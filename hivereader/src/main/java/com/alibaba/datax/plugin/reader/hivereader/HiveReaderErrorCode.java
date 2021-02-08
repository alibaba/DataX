package com.alibaba.datax.plugin.reader.hivereader;

import com.alibaba.datax.common.spi.ErrorCode;


public enum HiveReaderErrorCode implements ErrorCode {

    BAD_CONFIG_VALUE("HiveReader-00", "您配置的值不合法."),
    SQL_NOT_FOUND_ERROR("HiveReader-01", "您未配置hive sql"),
    DEFAULT_FS_NOT_FIND_ERROR("HiveReader-02", "您未配置defaultFS值"),
    ILLEGAL_VALUE("HiveReader-03", "值错误"),
    CONFIG_INVALID_EXCEPTION("HiveReader-04", "参数配置错误"),
    REQUIRED_VALUE("HiveReader-05", "您缺失了必须填写的参数值."),
    CONN_DB_ERROR("HiveReader-06", "连接hive出错."),
    FILE_TYPE_ERROR("HiveReader-07", "文件类型配置错误"),
    NO_FILE_FOUND("HiveReader-08", "未找到对应文件"),
    PATH_CONFIG_ERROR("HdfsReader-09", "您配置的path格式有误"),
    READ_FILE_ERROR("HdfsReader-10", "读取文件出错"),
    FILE_TYPE_UNSUPPORT("HdfsReader-12", "文件类型目前不支持"),
    KERBEROS_LOGIN_ERROR("HdfsReader-13", "KERBEROS认证失败"),
    READ_SEQUENCEFILE_ERROR("HdfsReader-14", "读取SequenceFile文件出错"),
    READ_RCFILE_ERROR("HdfsReader-15", "读取RCFile文件出错"),
    UNSUPPORTED_TYPE("HiveReader-16", "不支持的数据库字段类型"),
    SQL_EXECUTE_FAIL("HiveReader-17", "SQL执行出错");


    private final String code;
    private final String description;

    private HiveReaderErrorCode(String code, String description) {
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
