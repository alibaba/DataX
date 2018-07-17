package com.alibaba.datax.plugin.writer.hbase11xwriter;

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
        throw DataXException.asDataXException(Hbase11xWriterErrorCode.ILLEGAL_VALUE,
                String.format("Hbasewriter 不支持该 nullMode 类型:%s, 目前支持的 nullMode 类型是:%s", modeName, Arrays.asList(values())));
    }
}
