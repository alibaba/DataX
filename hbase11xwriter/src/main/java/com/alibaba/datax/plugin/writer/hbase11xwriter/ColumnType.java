package com.alibaba.datax.plugin.writer.hbase11xwriter;

import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.lang.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;

/**
 * 只对 normal 模式读取时有用，多版本读取时，不存在列类型的
 */
public enum ColumnType {
    STRING("string"),
    BOOLEAN("boolean"),
    SHORT("short"),
    INT("int"),
    LONG("long"),
    FLOAT("float"),
    DOUBLE("double"),
    $DEFAULT("$"),
    $UNSIGNED_INT("$unsigned_int"),
    $UNSIGNED_LONG("$unsigned_long"),
    $UNSIGNED_TINYINT("$unsigned_tinyint"),
    $UNSIGNED_SMALLINT("$unsigned_smallint"),
    $UNSIGNED_FLOAT("$unsigned_float"),
    $UNSIGNED_DOUBLE("$unsigned_double"),
    $INTEGER("$integer"),
    $BIGINT("$bigint"),
    $TINYINT("$tinyint"),
    $SMALLINT("$smallint"),
    $FLOAT("$float"),
    $DOUBLE("$double"),
    $DECIMAL("$decimal"),
    $BOOLEAN("$boolean"),
    $UNSIGNED_TIME("$unsigned_time"),
    $UNSIGNED_DATE("$unsigned_date"),
    $UNSIGNED_TIMESTAMP("$unsigned_timestamp"),
    $TIME("$time"),
    $DATE("$date"),
    $TIMESTAMP("$timestamp"),
    $VARBINARY("$varbinary"),
    $VARCHAR("$varchar")
    ;

    private String typeName;

    ColumnType(String typeName) {
        this.typeName = typeName;
    }

    public static ColumnType getByTypeName(String typeName) {
        if(StringUtils.isBlank(typeName)){
            throw DataXException.asDataXException(Hbase11xWriterErrorCode.ILLEGAL_VALUE,
                    String.format("Hbasewriter 不支持该类型:%s, 目前支持的类型是:%s", typeName, Arrays.asList(values())));
        }
        for (ColumnType columnType : values()) {
            if (StringUtils.equalsIgnoreCase(columnType.typeName, typeName.trim())) {
                return columnType;
            }
        }

        throw DataXException.asDataXException(Hbase11xWriterErrorCode.ILLEGAL_VALUE,
                String.format("Hbasewriter 不支持该类型:%s, 目前支持的类型是:%s", typeName, Arrays.asList(values())));
    }

    @Override
    public String toString() {
        return this.typeName;
    }

    public static ColumnType getPhoenixType(Class<?> javaType) {
        if (javaType == null) return $DEFAULT;
        ColumnType phType;
        if (Integer.class == javaType || int.class == javaType) {
            phType = $INTEGER;
        } else if (Long.class == javaType || long.class == javaType) {
            phType = $BIGINT;
        } else if (Byte.class == javaType || byte.class == javaType) {
            phType = $TINYINT;
        } else if (Short.class == javaType || short.class == javaType) {
            phType = $SMALLINT;
        } else if (Float.class == javaType || float.class == javaType) {
            phType = $FLOAT;
        } else if (Double.class == javaType || double.class == javaType) {
            phType = $DOUBLE;
        } else if (Boolean.class == javaType || boolean.class == javaType) {
            phType = $BOOLEAN;
        } else if (java.sql.Date.class == javaType) {
            phType = $DATE;
        } else if (Time.class == javaType) {
            phType = $DATE;
        } else if (Timestamp.class == javaType) {
            phType = $TIMESTAMP;
        } else if (Date.class == javaType) {
            phType = $DATE;
        } else if (byte[].class == javaType) {
            phType = $VARBINARY;
        } else if (String.class == javaType) {
            phType = $VARCHAR;
        } else if (BigDecimal.class == javaType) {
            phType = $DECIMAL;
        } else if (BigInteger.class == javaType) {
            phType = $UNSIGNED_LONG;
        } else {
            phType = $DEFAULT;
        }
        return phType;
    }
}
