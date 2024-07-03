package com.alibaba.datax.plugin.writer.obhbasewriter;

import java.util.Arrays;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.MessageSource;

public enum NullModeType {
    Skip("skip"),
    Empty("empty");

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
        throw DataXException.asDataXException(Hbase094xWriterErrorCode.ILLEGAL_VALUE, MessageSource.loadResourceBundle(NullModeType.class).message("nullmodetype.1", modeName, Arrays.asList(values())));
    }
}
