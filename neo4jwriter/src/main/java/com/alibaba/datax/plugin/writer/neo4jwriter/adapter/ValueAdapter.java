package com.alibaba.datax.plugin.writer.neo4jwriter.adapter;


import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.plugin.writer.neo4jwriter.config.Neo4jProperty;
import com.alibaba.datax.plugin.writer.neo4jwriter.element.PropertyType;
import com.alibaba.fastjson2.JSON;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.value.NullValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author fuyouj
 */
public class ValueAdapter {


    public static Value column2Value(final Column column, final Neo4jProperty neo4JProperty) {

        String typeStr = neo4JProperty.getType();
        PropertyType type = PropertyType.fromStrIgnoreCase(typeStr);
        if (column.asString() == null) {
            return NullValue.NULL;
        }

        switch (type) {
            case NULL:
                return NullValue.NULL;
            case MAP:
                return Values.value(JSON.parseObject(column.asString(), Map.class));
            case BOOLEAN:
                return Values.value(column.asBoolean());
            case STRING:
                return Values.value(column.asString());
            case INTEGER:
            case LONG:
                return Values.value(column.asLong());
            case SHORT:
                return Values.value(Short.valueOf(column.asString()));
            case FLOAT:
            case DOUBLE:
                return Values.value(column.asDouble());
            case BYTE_ARRAY:
                return Values.value(parseArrayType(neo4JProperty, column.asString(), Byte::valueOf));
            case CHAR_ARRAY:
                return Values.value(parseArrayType(neo4JProperty, column.asString(), (s) -> s.charAt(0)));
            case BOOLEAN_ARRAY:
                return Values.value(parseArrayType(neo4JProperty, column.asString(), Boolean::valueOf));
            case STRING_ARRAY:
            case Object_ARRAY:
            case LIST:
                return Values.value(parseArrayType(neo4JProperty, column.asString(), Function.identity()));
            case LONG_ARRAY:
                return Values.value(parseArrayType(neo4JProperty, column.asString(), Long::valueOf));
            case INT_ARRAY:
                return Values.value(parseArrayType(neo4JProperty, column.asString(), Integer::valueOf));
            case SHORT_ARRAY:
                return Values.value(parseArrayType(neo4JProperty, column.asString(), Short::valueOf));
            case DOUBLE_ARRAY:
            case FLOAT_ARRAY:
                return Values.value(parseArrayType(neo4JProperty, column.asString(), Double::valueOf));
            case LOCAL_DATE:
                return Values.value(DateAdapter.localDate(column.asString(), neo4JProperty));
            case LOCAL_TIME:
                return Values.value(DateAdapter.localTime(column.asString(), neo4JProperty));
            case LOCAL_DATE_TIME:
                return Values.value(DateAdapter.localDateTime(column.asString(), neo4JProperty));
            default:
                return Values.value(column.getRawData());

        }
    }


    private static <R> List<R> parseArrayType(final Neo4jProperty neo4JProperty,
                                              final String strValue,
                                              final Function<String, R> convertFunc) {
        if (null == strValue || "".equals(strValue)) {
            return Collections.emptyList();
        }
        String split = neo4JProperty.getSplitOrDefault();
        String[] strArr = strValue.split(split);
        List<R> ans = new ArrayList<>();
        for (String s : strArr) {
            ans.add(convertFunc.apply(s));
        }
        return ans;
    }
}
