package com.alibaba.datax.plugin.reader.obhbasereader.enums;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.reader.obhbasereader.HbaseReaderErrorCode;

import java.util.Arrays;

/**
 * 只对 normal 模式读取时有用，多版本读取时，不存在列类型的
 */
public enum ColumnType {
    STRING("string"),
    BINARY_STRING("binarystring"),
    BYTES("bytes"),
    BOOLEAN("boolean"),
    SHORT("short"),
    INT("int"),
    LONG("long"),
    FLOAT("float"),
    DOUBLE("double"),
    DATE("date");

    private String typeName;

    ColumnType(String typeName) {
        this.typeName = typeName;
    }

    public static ColumnType getByTypeName(String typeName) {
        for (ColumnType columnType : values()) {
            if (columnType.typeName.equalsIgnoreCase(typeName)) {
                return columnType;
            }
        }

        throw DataXException.asDataXException(HbaseReaderErrorCode.ILLEGAL_VALUE,
                String.format("The type %s is not supported by hbasereader, currently supported type is:%s .", typeName, Arrays.asList(values())));
    }

    @Override
    public String toString() {
        return this.typeName;
    }
}
