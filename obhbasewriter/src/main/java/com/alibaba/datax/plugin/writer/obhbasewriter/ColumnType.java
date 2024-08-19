package com.alibaba.datax.plugin.writer.obhbasewriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.MessageSource;
import java.util.Arrays;



import org.apache.commons.lang.StringUtils;

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
    DATE("date"),
    BINARY("binary");

    private String typeName;

    ColumnType(String typeName) {
        this.typeName = typeName;
    }

    public static ColumnType getByTypeName(String typeName) {
        if (StringUtils.isBlank(typeName)) {
            throw DataXException.asDataXException(Hbase094xWriterErrorCode.ILLEGAL_VALUE, MessageSource.loadResourceBundle(ColumnType.class).message("columntype.1", typeName, Arrays.asList(values())));
        }
        for (ColumnType columnType : values()) {
            if (StringUtils.equalsIgnoreCase(columnType.typeName, typeName.trim())) {
                return columnType;
            }
        }

        throw DataXException.asDataXException(Hbase094xWriterErrorCode.ILLEGAL_VALUE, MessageSource.loadResourceBundle(ColumnType.class).message("columntype.1", typeName, Arrays.asList(values())));
    }

    @Override
    public String toString() {
        return this.typeName;
    }
}
