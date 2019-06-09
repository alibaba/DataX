

package com.alibaba.datax.common.constant;

import java.sql.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Summary：<p></p>
 * Author : Martin
 * Since  : 2019/5/18 19:33
 */
public enum JavaDataType
{
    /**
     * CHAR -> String -> String
     * VARCHAR -> String -> String
     * LONGVARCHAR -> String -> String
     * BIT -> boolean -> Boolean
     * TINYINT -> byte -> Integer
     * SMALLINT -> short -> Integer
     * INTEGER -> int -> Integer
     * BIGINT -> long -> Long
     * REAL -> float -> Float
     * FLOAT -> double -> Double
     * DOUBLE -> double -> Double
     * BINARY -> byte[] -> byte[]
     * VARBINARY -> byte[] -> byte[]
     * LONGVARBINARY -> byte[] -> byte[]
     * NUMERIC -> java.math.BigDecimal -> java.math.BigDecimal
     * DECIMAL -> java.math.BigDecimal -> java.math.BigDecimal
     * DATE -> java.sql.Date -> java.sql.Date
     * TIME -> java.sql.Time -> java.sql.Time
     * TIMESTAMP -> java.sql.Timestamp -> java.sql.Timestamp
     */
    BYTE(Byte.class.getName()),
    SHORT(Short.class.getName()),
    INTEGER(Integer.class.getName()),
    LONG(Long.class.getName()),
    FLOAT(Float.class.getName()),
    DOUBLE(Double.class.getName()),
    CHAR(Character.class.getName()),
    STRING(String.class.getName()),
    BOOLEAN(Boolean.class.getName()),
    DATE(Date.class.getName()),
    TIME(Time.class.getName()),
    TIMESTAMP(Timestamp.class.getName()),
    BIGDECIMAL(java.math.BigDecimal.class.getName()),
    ARRAY(Array.class.getName()),
    NULL("null"),
    BADTYPE("BadType");

    private String clzName;

    JavaDataType(String clzName)
    {
        this.clzName = clzName;
    }

    public String getClzName()
    {
        return clzName;
    }

    @Override
    public String toString()
    {
        return "JavaDataType{" +
                "clzName='" + clzName + '\'' +
                '}';
    }
}
