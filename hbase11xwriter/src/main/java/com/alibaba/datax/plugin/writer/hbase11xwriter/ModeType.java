package com.alibaba.datax.plugin.writer.hbase11xwriter;

import com.alibaba.datax.common.exception.DataXException;

import java.util.Arrays;

public enum ModeType {
    Normal("normal"),
    MultiVersion("multiVersion")
    ;

    private String mode;


    ModeType(String mode) {
        this.mode = mode.toLowerCase();
    }

    public String getMode() {
        return mode;
    }

    public static ModeType getByTypeName(String modeName) {
        for (ModeType modeType : values()) {
            if (modeType.mode.equalsIgnoreCase(modeName)) {
                return modeType;
            }
        }
        throw DataXException.asDataXException(Hbase11xWriterErrorCode.ILLEGAL_VALUE,
                String.format("Hbasewriter 不支持该 mode 类型:%s, 目前支持的 mode 类型是:%s", modeName, Arrays.asList(values())));
    }
}
