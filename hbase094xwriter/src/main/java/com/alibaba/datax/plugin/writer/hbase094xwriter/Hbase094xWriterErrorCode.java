package com.alibaba.datax.plugin.writer.hbase094xwriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by shf on 16/3/8.
 */
public enum Hbase094xWriterErrorCode implements ErrorCode {
    REQUIRED_VALUE("Hbasewriter-00", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("Hbasewriter-01", "您填写的参数值不合法."),
    GET_HBASE_CONFIG_ERROR("Hbasewriter-02", "获取Hbase config时出错."),
    GET_HBASE_TABLE_ERROR("Hbasewriter-03", "初始化 Hbase 抽取表时出错."),
    CLOSE_HBASE_AMIN_ERROR("Hbasewriter-05", "关闭Hbase admin时出错."),
    CLOSE_HBASE_TABLE_ERROR("Hbasewriter-06", "关闭Hbase table时时出错."),
    PUT_HBASE_ERROR("Hbasewriter-07", "写入hbase时发生IO异常."),
    DELETE_HBASE_ERROR("Hbasewriter-08", "delete hbase表时发生异常."),
    TRUNCATE_HBASE_ERROR("Hbasewriter-09", "truncate hbase表时发生异常"),
    CONSTRUCT_ROWKEY_ERROR("Hbasewriter-10", "构建rowkey时发生异常."),
    CONSTRUCT_VERSION_ERROR("Hbasewriter-11", "构建version时发生异常.")
    ;
    private final String code;
    private final String description;

    private Hbase094xWriterErrorCode(String code, String description) {
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
