package com.alibaba.datax.plugin.reader.hbase094xreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum Hbase094xReaderErrorCode implements ErrorCode {
    REQUIRED_VALUE("Hbase094xReader-00", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("Hbase094xReader-01", "您配置的值不合法."),
    PREPAR_READ_ERROR("Hbase094xReader-02", "准备读取 Hbase 时出错."),
    SPLIT_ERROR("Hbase094xReader-03", "切分 Hbase 表时出错."),
    GET_HBASE_CONFIGURATION_ERROR("HbaseReader-04", "解析hbase configuration时出错."),
    INIT_TABLE_ERROR("Hbase094xReader-04", "初始化 Hbase 抽取表时出错."),
    GET_HBASE_TABLE_ERROR("HbaseReader-05", "初始化 Hbase 抽取表时出错."),
    CLOSE_HBASE_TABLE_ERROR("HbaseReader-06", "关闭Hbase 抽取表时出错."),
    CLOSE_HBASE_ADMIN_ERROR("HbaseReader-07", "关闭 Hbase admin时出错.")
    ;

    private final String code;
    private final String description;

    private Hbase094xReaderErrorCode(String code, String description) {
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
