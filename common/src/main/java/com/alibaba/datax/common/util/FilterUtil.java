package com.alibaba.datax.common.util;

import java.util.*;
import java.util.regex.Pattern;

/**
 * 提供从 List<String> 中根据 regular 过滤的通用工具(返回值已经去重). 使用场景，比如：odpsreader
 * 的分区筛选，hdfsreader/txtfilereader的路径筛选等
 */
public final class FilterUtil {

    //已经去重
    public static List<String> filterByRegular(List<String> allStrs,
                                               String regular) {
        List<String> matchedValues = new ArrayList<String>();

        // 语法习惯上的兼容处理(pt=* 实际正则应该是：pt=.*)
        String newReqular = regular.replace(".*", "*").replace("*", ".*");

        Pattern p = Pattern.compile(newReqular);

        for (String partition : allStrs) {
            if (p.matcher(partition).matches()) {
                if (!matchedValues.contains(partition)) {
                    matchedValues.add(partition);
                }
            }
        }

        return matchedValues;
    }

    //已经去重
    public static List<String> filterByRegulars(List<String> allStrs,
                                                List<String> regulars) {
        List<String> matchedValues = new ArrayList<String>();

        List<String> tempMatched = null;
        for (String regular : regulars) {
            tempMatched = filterByRegular(allStrs, regular);
            if (null != tempMatched && !tempMatched.isEmpty()) {
                for (String temp : tempMatched) {
                    if (!matchedValues.contains(temp)) {
                        matchedValues.add(temp);
                    }
                }
            }
        }

        return matchedValues;
    }
}
