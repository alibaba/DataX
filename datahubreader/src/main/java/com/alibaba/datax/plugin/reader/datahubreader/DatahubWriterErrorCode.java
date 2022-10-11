package com.alibaba.datax.plugin.reader.datahubreader;

import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.datax.common.util.MessageSource;

public enum DatahubWriterErrorCode implements ErrorCode {
    MISSING_REQUIRED_VALUE("DatahubWriter-01", MessageSource.loadResourceBundle(DatahubWriterErrorCode.class).message("errorcode.missing_required_value")),
    INVALID_CONFIG_VALUE("DatahubWriter-02", MessageSource.loadResourceBundle(DatahubWriterErrorCode.class).message("errorcode.invalid_config_value")),
    GET_TOPOIC_INFO_FAIL("DatahubWriter-03", MessageSource.loadResourceBundle(DatahubWriterErrorCode.class).message("errorcode.get_topic_info_fail")),
    WRITE_DATAHUB_FAIL("DatahubWriter-04", MessageSource.loadResourceBundle(DatahubWriterErrorCode.class).message("errorcode.write_datahub_fail")),
    SCHEMA_NOT_MATCH("DatahubWriter-05", MessageSource.loadResourceBundle(DatahubWriterErrorCode.class).message("errorcode.schema_not_match")),
    ;

    private final String code;
    private final String description;

    private DatahubWriterErrorCode(String code, String description) {
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