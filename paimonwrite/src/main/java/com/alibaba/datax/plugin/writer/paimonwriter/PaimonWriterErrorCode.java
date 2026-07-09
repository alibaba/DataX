package com.alibaba.datax.plugin.writer.paimonwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum PaimonWriterErrorCode implements ErrorCode {
    PAIMON_ERROR_DB("Paimon Error DB", "您的参数配置错误."),
    PAIMON_ERROR_TABLE("Paimon Error Table", "您的参数配置错误."),
    PAIMON_PARAM_LOST("Paimon Param Lost", "您缺失了必须填写的参数值."),
    HDFS_CONNECT_ERROR("Hdfs Connect Error", "与HDFS建立连接时出现IO异常."),
    KERBEROS_LOGIN_ERROR("Hdfs Login Error", "KERBEROS认证失败");

    private final String code;
    private final String description;

    PaimonWriterErrorCode(String code, String description) {
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
