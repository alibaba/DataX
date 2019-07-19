package com.alibaba.datax.plugin.writer.hbase20xsqlwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum HBase20xSQLWriterErrorCode implements ErrorCode {
    REQUIRED_VALUE("Hbasewriter-00", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("Hbasewriter-01", "您填写的参数值不合法."),
    GET_QUERYSERVER_CONNECTION_ERROR("Hbasewriter-02", "获取QueryServer连接时出错."),
    GET_HBASE_TABLE_ERROR("Hbasewriter-03", "获取 Hbase table时出错."),
    CLOSE_HBASE_CONNECTION_ERROR("Hbasewriter-04", "关闭Hbase连接时出错."),
    GET_TABLE_COLUMNTYPE_ERROR("Hbasewriter-05", "获取表列类型时出错."),
    PUT_HBASE_ERROR("Hbasewriter-07", "写入hbase时发生IO异常."),
            ;

    private final String code;
    private final String description;

    private HBase20xSQLWriterErrorCode(String code, String description) {
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
