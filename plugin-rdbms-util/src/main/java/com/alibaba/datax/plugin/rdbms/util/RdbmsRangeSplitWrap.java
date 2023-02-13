package com.alibaba.datax.plugin.rdbms.util;

import com.alibaba.datax.common.util.RangeSplitUtil;
import org.apache.commons.lang3.StringUtils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public final class RdbmsRangeSplitWrap {

    public static List<String> splitAndWrap(String left, String right, int expectSliceNumber,
                                            String columnName, String quote, DataBaseType dataBaseType) {
        String[] tempResult;
        //如果key为UUID类型
        if (keyIsUUID(left, right)) {
            tempResult = RangeSplitUtil.doUUIDStringSplit(left, right, expectSliceNumber);
        } else {
            tempResult = RangeSplitUtil.doAsciiStringSplit(left, right, expectSliceNumber);
        }
        return RdbmsRangeSplitWrap.wrapRange(tempResult, columnName, quote, dataBaseType);
    }

    private static Boolean keyIsUUID(String left, String right) {
        String UUID_regex = "[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}";
        Pattern leftPattern = Pattern.compile(UUID_regex);
        boolean UUID_Underline = leftPattern.matcher(left).matches() && leftPattern.matcher(right).matches();
        Pattern rightPattern = Pattern.compile("[0-9a-f]{32}$", Pattern.CASE_INSENSITIVE);
        boolean UUID = rightPattern.matcher(left).matches() && rightPattern.matcher(right).matches();
        return UUID || UUID_Underline;
    }

    // warn: do not use this method long->BigInteger
    public static List<String> splitAndWrap(long left, long right, int expectSliceNumber, String columnName) {
        long[] tempResult = RangeSplitUtil.doLongSplit(left, right, expectSliceNumber);
        return RdbmsRangeSplitWrap.wrapRange(tempResult, columnName);
    }

    public static List<String> splitAndWrap(BigInteger left, BigInteger right, int expectSliceNumber, String columnName) {
        BigInteger[] tempResult = RangeSplitUtil.doBigIntegerSplit(left, right, expectSliceNumber);
        return RdbmsRangeSplitWrap.wrapRange(tempResult, columnName);
    }

    public static List<String> wrapRange(long[] rangeResult, String columnName) {
        String[] rangeStr = new String[rangeResult.length];
        for (int i = 0, len = rangeResult.length; i < len; i++) {
            rangeStr[i] = String.valueOf(rangeResult[i]);
        }
        return wrapRange(rangeStr, columnName, "", null);
    }

    public static List<String> wrapRange(BigInteger[] rangeResult, String columnName) {
        String[] rangeStr = new String[rangeResult.length];
        for (int i = 0, len = rangeResult.length; i < len; i++) {
            rangeStr[i] = rangeResult[i].toString();
        }
        return wrapRange(rangeStr, columnName, "", null);
    }

    public static List<String> wrapRange(String[] rangeResult, String columnName,
                                         String quote, DataBaseType dataBaseType) {
        if (null == rangeResult || rangeResult.length < 2) {
            throw new IllegalArgumentException(String.format(
                    "Parameter rangeResult can not be null and its length can not <2. detail:rangeResult=[%s].",
                    StringUtils.join(rangeResult, ",")));
        }

        List<String> result = new ArrayList<String>();

        //TODO  change to  stringbuilder.append(..)
        if (2 == rangeResult.length) {
            result.add(String.format(" (%s%s%s <= %s AND %s <= %s%s%s) ", quote, quoteConstantValue(rangeResult[0], dataBaseType),
                    quote, columnName, columnName, quote, quoteConstantValue(rangeResult[1], dataBaseType), quote));
            return result;
        } else {
            for (int i = 0, len = rangeResult.length - 2; i < len; i++) {
                result.add(String.format(" (%s%s%s <= %s AND %s < %s%s%s) ", quote, quoteConstantValue(rangeResult[i], dataBaseType),
                        quote, columnName, columnName, quote, quoteConstantValue(rangeResult[i + 1], dataBaseType), quote));
            }

            result.add(String.format(" (%s%s%s <= %s AND %s <= %s%s%s) ", quote, quoteConstantValue(rangeResult[rangeResult.length - 2], dataBaseType),
                    quote, columnName, columnName, quote, quoteConstantValue(rangeResult[rangeResult.length - 1], dataBaseType), quote));
            return result;
        }
    }
    
    public static String wrapFirstLastPoint(String firstPoint, String lastPoint, String columnName,
            String quote, DataBaseType dataBaseType) {
        return String.format(" ((%s < %s%s%s) OR (%s%s%s < %s)) ", columnName, quote, quoteConstantValue(firstPoint, dataBaseType),
                quote, quote, quoteConstantValue(lastPoint, dataBaseType), quote, columnName);
    }
    
    public static String wrapFirstLastPoint(Long firstPoint, Long lastPoint, String columnName) {
        return wrapFirstLastPoint(firstPoint.toString(), lastPoint.toString(), columnName, "", null);
    }

    public static String wrapFirstLastPoint(BigInteger firstPoint, BigInteger lastPoint, String columnName) {
        return wrapFirstLastPoint(firstPoint.toString(), lastPoint.toString(), columnName, "", null);
    }
    

    private static String quoteConstantValue(String aString, DataBaseType dataBaseType) {
        if (null == dataBaseType) {
            return aString;
        }

        if (dataBaseType.equals(DataBaseType.MySql)) {
            return aString.replace("'", "''").replace("\\", "\\\\");
        } else if (dataBaseType.equals(DataBaseType.Oracle) || dataBaseType.equals(DataBaseType.SQLServer)) {
            return aString.replace("'", "''");
        } else {
            //TODO other type supported
            return aString;
        }
    }
}
