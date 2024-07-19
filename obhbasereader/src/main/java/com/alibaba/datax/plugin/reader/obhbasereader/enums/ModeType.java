package com.alibaba.datax.plugin.reader.obhbasereader.enums;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.reader.obhbasereader.HbaseReaderErrorCode;

import java.util.Arrays;

public enum ModeType {
    Normal("normal"),
    MultiVersionFixedColumn("multiVersionFixedColumn"),
    MultiVersionDynamicColumn("multiVersionDynamicColumn"),
    ;

    private String mode;

    ModeType(String mode) {
        this.mode = mode.toLowerCase();
    }

    public static ModeType getByTypeName(String modeName) {
        for (ModeType modeType : values()) {
            if (modeType.mode.equalsIgnoreCase(modeName)) {
                return modeType;
            }
        }

        throw DataXException.asDataXException(
                HbaseReaderErrorCode.ILLEGAL_VALUE, String.format("The mode type is not supported by hbasereader:%s, and the currently supported mode type is:%s", modeName, Arrays.asList(values())));
    }
}
