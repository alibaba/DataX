package com.alibaba.datax.plugin.writer.obhbasewriter;

import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.datax.common.util.MessageSource;

/**
 * Created by shf on 16/3/8.
 */
public enum Hbase094xWriterErrorCode implements ErrorCode {
    REQUIRED_VALUE("Hbasewriter-00", MessageSource.loadResourceBundle(Hbase094xWriterErrorCode.class).message("errorcode.required_value")),
    ILLEGAL_VALUE("Hbasewriter-01", MessageSource.loadResourceBundle(Hbase094xWriterErrorCode.class).message("errorcode.illegal_value")),
    GET_HBASE_CONFIG_ERROR("Hbasewriter-02", MessageSource.loadResourceBundle(Hbase094xWriterErrorCode.class).message("errorcode.get_hbase_config_error")),
    GET_HBASE_TABLE_ERROR("Hbasewriter-03", MessageSource.loadResourceBundle(Hbase094xWriterErrorCode.class).message("errorcode.get_hbase_table_error")),
    CLOSE_HBASE_AMIN_ERROR("Hbasewriter-05", MessageSource.loadResourceBundle(Hbase094xWriterErrorCode.class).message("errorcode.close_hbase_amin_error")),
    CLOSE_HBASE_TABLE_ERROR("Hbasewriter-06", MessageSource.loadResourceBundle(Hbase094xWriterErrorCode.class).message("errorcode.close_hbase_table_error")),
    PUT_HBASE_ERROR("Hbasewriter-07", MessageSource.loadResourceBundle(Hbase094xWriterErrorCode.class).message("errorcode.put_hbase_error")),
    DELETE_HBASE_ERROR("Hbasewriter-08", MessageSource.loadResourceBundle(Hbase094xWriterErrorCode.class).message("errorcode.delete_hbase_error")),
    TRUNCATE_HBASE_ERROR("Hbasewriter-09", MessageSource.loadResourceBundle(Hbase094xWriterErrorCode.class).message("errorcode.truncate_hbase_error")),
    CONSTRUCT_ROWKEY_ERROR("Hbasewriter-10", MessageSource.loadResourceBundle(Hbase094xWriterErrorCode.class).message("errorcode.construct_rowkey_error")),
    CONSTRUCT_VERSION_ERROR("Hbasewriter-11", MessageSource.loadResourceBundle(Hbase094xWriterErrorCode.class).message("errorcode.construct_version_error")),
    INIT_ERROR("Hbasewriter-12", MessageSource.loadResourceBundle(Hbase094xWriterErrorCode.class).message("errorcode.init_error"));
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
        return String.format("Code:[%s], Description:[%s].", this.code, this.description);
    }
}
