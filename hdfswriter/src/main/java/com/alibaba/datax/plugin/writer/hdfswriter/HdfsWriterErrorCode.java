package com.alibaba.datax.plugin.writer.hdfswriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by shf on 15/10/8.
 */
public enum HdfsWriterErrorCode implements ErrorCode {

    CONFIG_INVALID_EXCEPTION("HdfsWriter-00", "您的参数配置错误."),
    REQUIRED_VALUE("HdfsWriter-01", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("HdfsWriter-02", "您填写的参数值不合法."),
    WRITER_FILE_WITH_CHARSET_ERROR("HdfsWriter-03", "您配置的编码未能正常写入."),
    Write_FILE_IO_ERROR("HdfsWriter-04", "您配置的文件在写入时出现IO异常."),
    WRITER_RUNTIME_EXCEPTION("HdfsWriter-05", "出现运行时异常, 请联系我们."),
    CONNECT_HDFS_IO_ERROR("HdfsWriter-06", "与HDFS建立连接时出现IO异常."),
    COLUMN_REQUIRED_VALUE("HdfsWriter-07", "您column配置中缺失了必须填写的参数值."),
    HDFS_RENAME_FILE_ERROR("HdfsWriter-08", "将文件移动到配置路径失败."),
    KERBEROS_LOGIN_ERROR("HdfsWriter-09", "KERBEROS认证失败");

    private final String code;
    private final String description;

    private HdfsWriterErrorCode(String code, String description) {
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
