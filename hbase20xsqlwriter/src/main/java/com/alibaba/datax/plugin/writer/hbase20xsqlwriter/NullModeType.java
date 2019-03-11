package com.alibaba.datax.plugin.writer.hbase20xsqlwriter;

import com.alibaba.datax.common.exception.DataXException;

import java.util.Arrays;

public enum NullModeType {
    Skip("skip"),
    Empty("empty")
    ;

    private String mode;


    NullModeType(String mode) {
        this.mode = mode.toLowerCase();
    }

    public String getMode() {
        return mode;
    }

    public static NullModeType getByTypeName(String modeName) {
        for (NullModeType modeType : values()) {
            if (modeType.mode.equalsIgnoreCase(modeName)) {
                return modeType;
            }
        }
        throw DataXException.asDataXException(HBase20xSQLWriterErrorCode.ILLEGAL_VALUE,
                "Hbasewriter 不支持该 nullMode 类型:" + modeName + ", 目前支持的 nullMode 类型是:" + Arrays.asList(values()));
    }
}
