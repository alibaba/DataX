package com.alibaba.datax.plugin.writer.obhbasewriter;

import java.util.Arrays;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.MessageSource;

public enum ModeType {
    Normal("normal"),
    MultiVersion("multiVersion");

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
        throw DataXException.asDataXException(Hbase094xWriterErrorCode.ILLEGAL_VALUE, MessageSource.loadResourceBundle(ModeType.class).message("modetype.1", modeName, Arrays.asList(values())));
    }
}
