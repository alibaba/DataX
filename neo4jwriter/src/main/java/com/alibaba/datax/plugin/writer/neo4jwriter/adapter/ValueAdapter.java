package com.alibaba.datax.plugin.writer.neo4jwriter.adapter;


import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.plugin.writer.neo4jwriter.config.Neo4jField;
import com.alibaba.datax.plugin.writer.neo4jwriter.element.FieldType;
import com.alibaba.fastjson2.JSON;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.value.NullValue;

import java.util.*;
import java.util.function.Function;

/**
 * @author fuyouj
 */
public class ValueAdapter {


    public static Value column2Value(final Column column, final Neo4jField neo4jField) {
        FieldType type = neo4jField.getFieldType();
        switch (type) {
            case NULL:
                return NullValue.NULL;
            case MAP:
                return Values.value(JSON.parseObject(column.asString(),Map.class));
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
                return Values.value(parseArrayType(neo4jField, column.asString(), Byte::valueOf));
            case CHAR_ARRAY:
                return Values.value(parseArrayType(neo4jField, column.asString(), (s) -> s.charAt(0)));
            case BOOLEAN_ARRAY:
                return Values.value(parseArrayType(neo4jField, column.asString(), Boolean::valueOf));
            case STRING_ARRAY:
            case Object_ARRAY:
            case LIST:
                return Values.value(parseArrayType(neo4jField, column.asString(), Function.identity()));
            case LONG_ARRAY:
                return Values.value(parseArrayType(neo4jField, column.asString(), Long::valueOf));
            case INT_ARRAY:
                return Values.value(parseArrayType(neo4jField, column.asString(), Integer::valueOf));
            case SHORT_ARRAY:
                return Values.value(parseArrayType(neo4jField, column.asString(), Short::valueOf));
            case DOUBLE_ARRAY:
            case FLOAT_ARRAY:
                return Values.value(parseArrayType(neo4jField, column.asString(), Double::valueOf));
            case LOCAL_DATE:
                return Values.value(DateAdapter.localDate(column.asString(), neo4jField));
            case LOCAL_TIME:
                return Values.value(DateAdapter.localTime(column.asString(),neo4jField));
            case LOCAL_DATE_TIME:
                return Values.value(DateAdapter.localDateTime(column.asString(),neo4jField));
            default:
                return Values.value(column.getRawData());

        }
    }


    private static <R> List<R> parseArrayType(final Neo4jField neo4jField,
                                              final String strValue,
                                              final Function<String, R> convertFunc) {
        if (null == strValue || "".equals(strValue)) {
            return Collections.emptyList();
        }
        String trimStr = trimString(strValue, neo4jField.getArrayTrimOrDefault());
        if ("".equals(trimStr)) {
            return Collections.emptyList();
        }
        String[] strArr = trimStr.split(neo4jField.getSplitOrDefault());
        List<R> ans = new ArrayList<>();
        for (String s : strArr) {
            ans.add(convertFunc.apply(s));
        }
        return ans;
    }

    public static String trimString(String strValue, List<Character> trimChars) {

        Set<Character> characters = new HashSet<>(trimChars);
        char[] chars = strValue.toCharArray();
        int i = 0;
        int j = chars.length - 1;

        while (i <= chars.length - 1 && characters.contains(chars[i])) {
            i++;
        }
        while (j >= i && characters.contains(chars[j])) {
            j--;
        }

        if (i > j) {
            return "";
        }

        if (i == j) {
            return String.valueOf(chars[i]);
        }

        return new String(Arrays.copyOfRange(chars, i, j + 1));
    }
}
