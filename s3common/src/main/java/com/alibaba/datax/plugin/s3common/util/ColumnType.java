package com.alibaba.datax.plugin.s3common.util;

import cn.hutool.core.util.StrUtil;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Author: duhanmin
 * Description:
 * Date: 2021/5/19 15:32
 */
public enum ColumnType {

    /**
     * string type
     */
    STRING, VARCHAR, VARCHAR2, CHAR, NVARCHAR, TEXT, KEYWORD, BINARY,

    /**
     * number type
     */
    INT, INT32, MEDIUMINT, TINYINT, DATETIME, SMALLINT, BIGINT, LONG, INT64 , SHORT, INTEGER, NUMBER,

    /**
     * double type
     */
    DOUBLE, FLOAT,
    BOOLEAN,

    /**
     * date type
     */
    OTHER,

    /**
     * date type
     */
    DATE, TIMESTAMP, TIME,
    DECIMAL, YEAR, BIT;



    public static List<ColumnType> TIME_TYPE = Arrays.asList(
            DATE, DATETIME, TIME, TIMESTAMP
    );

    public static List<ColumnType> NUMBER_TYPE = Arrays.asList(
            INT, INTEGER, MEDIUMINT, TINYINT, SMALLINT, BIGINT, LONG, SHORT, DOUBLE, FLOAT, DECIMAL, NUMBER
    );

    public static List<ColumnType> STRING_TYPE = Arrays.asList(
            STRING, VARCHAR, VARCHAR2, CHAR, NVARCHAR, TEXT, KEYWORD, BINARY
    );

    /**
     * 根据字段类型的字符串找出对应的枚举
     * 找不到直接报错 IllegalArgumentException
     * @param type
     * @return
     */
    public static ColumnType fromString(String type) {
        if(type == null) {
            throw new RuntimeException("null ColumnType!");
        }

        if(type.contains(ConstantValue.LEFT_PARENTHESIS_SYMBOL)){
            type = type.substring(0, type.indexOf(ConstantValue.LEFT_PARENTHESIS_SYMBOL));
        }

        type =  type.toUpperCase(Locale.ENGLISH);
        //为了支持无符号类型  如 int unsigned
        if(StrUtil.contains(type,ConstantValue.DATA_TYPE_UNSIGNED)){
            type = type.replace(ConstantValue.DATA_TYPE_UNSIGNED,"").trim();
        }

        try {
            return valueOf(type);
        }catch (Exception e){
            return ColumnType.OTHER;
        }
    }

    /**
     * 根据字段类型的字符串找到对应的枚举 找不到就直接返回ColumnType.STRING;
     * @param type
     * @return
     */
    public static ColumnType getType(String type){
        type = type.toUpperCase(Locale.ENGLISH);
        if(type.contains(ConstantValue.LEFT_PARENTHESIS_SYMBOL)){
            type = type.substring(0, type.indexOf(ConstantValue.LEFT_PARENTHESIS_SYMBOL));
        }

        //为了支持无符号类型  如 int unsigned
        if(StrUtil.contains(type,ConstantValue.DATA_TYPE_UNSIGNED)){
            type = type.replaceAll(ConstantValue.DATA_TYPE_UNSIGNED,"").trim();
        }

        if(type.contains(ColumnType.TIMESTAMP.name())){
            return TIMESTAMP;
        }

        for (ColumnType value : ColumnType.values()) {
            if(type.equalsIgnoreCase(value.name())){
                return value;
            }
        }

        return ColumnType.STRING;
    }

    public static boolean isTimeType(String type){
        return TIME_TYPE.contains(getType(type));
    }

    public static boolean isNumberType(String type){
        return NUMBER_TYPE.contains(getType(type));
    }

    public static boolean isStringType(String type) {
        return STRING_TYPE.contains(getType(type));
    }

    public static boolean isStringType(ColumnType type) {
        return STRING_TYPE.contains(type);
    }
}

