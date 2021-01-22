package com.alibaba.datax.plugin.reader.kudureader;

import com.alibaba.datax.common.exception.DataXException;

import java.util.Arrays;

/**
 * @author daizihao
 * @create 2020-08-31 19:12
 **/
public enum ColumnType {
    INT("int"),
    FLOAT("float"),
    STRING("string"),
    BIGINT("bigint"),
    DOUBLE("double"),
    BOOLEAN("boolean"),
    LONG("long"),
    TIMESTAMP("timestamp"),
    DATE("date");
    private String mode;
    ColumnType(String mode) {
        this.mode = mode.toLowerCase();
    }

    public String getMode() {
        return mode;
    }

    public static ColumnType getByTypeName(String modeName) {
        for (ColumnType modeType : values()) {
            if (modeType.mode.equalsIgnoreCase(modeName)) {
                return modeType;
            }
        }
        throw DataXException.asDataXException(KuduReaderErrorcode.ILLEGAL_VALUE,
                String.format("Kudureader does not support the type:%s, currently supported types are:%s", modeName, Arrays.asList(values())));
    }
}
