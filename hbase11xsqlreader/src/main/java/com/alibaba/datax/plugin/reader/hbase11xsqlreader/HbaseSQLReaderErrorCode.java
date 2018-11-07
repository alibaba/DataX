package com.alibaba.datax.plugin.reader.hbase11xsqlreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum HbaseSQLReaderErrorCode implements ErrorCode {
    REQUIRED_VALUE("Hbasewriter-00", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("Hbasewriter-01", "您填写的参数值不合法."),
    GET_PHOENIX_COLUMN_ERROR("Hbasewriter-02", "获取phoenix表的列值错误"),
    GET_PHOENIX_CONNECTIONINFO_ERROR("Hbasewriter-03", "获取phoenix服务的zkurl错误"),
    GET_PHOENIX_SPLITS_ERROR("Hbasewriter-04", "获取phoenix的split信息错误"),
    PHOENIX_CREATEREADER_ERROR("Hbasewriter-05", "获取phoenix的reader错误"),
    PHOENIX_READERINIT_ERROR("Hbasewriter-06", "phoenix reader的初始化错误"),
    PHOENIX_COLUMN_TYPE_CONVERT_ERROR("Hbasewriter-07", "phoenix的列类型转换错误"),
    PHOENIX_RECORD_READ_ERROR("Hbasewriter-08", "phoenix record 读取错误"),
    PHOENIX_READER_CLOSE_ERROR("Hbasewriter-09", "phoenix reader 的close错误")
    ;

    private final String code;
    private final String description;

    private HbaseSQLReaderErrorCode(String code, String description) {
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
