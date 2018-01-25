package com.alibaba.datax.plugin.reader.hbase11xreader;

import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;

/**
 * 只对 normal 模式读取时有用，多版本读取时，不存在列类型的
 */
public enum ColumnType {
    BOOLEAN("boolean"),
    SHORT("short"),
    INT("int"),
    LONG("long"),
    FLOAT("float"),
    DOUBLE("double"),
    DATE("date"),
    STRING("string"),
    BINARY_STRING("binarystring")
    ;

    private String typeName;

    ColumnType(String typeName) {
        this.typeName = typeName;
    }

    public static ColumnType getByTypeName(String typeName) {
        if(StringUtils.isBlank(typeName)){
            throw DataXException.asDataXException(Hbase11xReaderErrorCode.ILLEGAL_VALUE,
                    String.format("Hbasereader 不支持该类型:%s, 目前支持的类型是:%s", typeName, Arrays.asList(values())));
        }
        for (ColumnType columnType : values()) {
            if (StringUtils.equalsIgnoreCase(columnType.typeName, typeName.trim())) {
                return columnType;
            }
        }

        throw DataXException.asDataXException(Hbase11xReaderErrorCode.ILLEGAL_VALUE,
                String.format("Hbasereader 不支持该类型:%s, 目前支持的类型是:%s", typeName, Arrays.asList(values())));
    }

    @Override
    public String toString() {
        return this.typeName;
    }
}
