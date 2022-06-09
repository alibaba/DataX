package com.alibaba.datax.common.util;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * 提供针对 DataX中使用的 List 较为常见的一些封装。 比如：checkIfValueDuplicate 可以用于检查用户配置的 writer
 * 的列不能重复。makeSureNoValueDuplicate亦然，只是会严格报错。
 */
public final class ListUtil {

    public static boolean checkIfValueDuplicate(List<String> aList,
                                                boolean caseSensitive) {
        if (null == aList || aList.isEmpty()) {
            throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR,
                    "您提供的作业配置有误，List不能为空.");
        }

        try {
            makeSureNoValueDuplicate(aList, caseSensitive);
        } catch (Exception e) {
            return true;
        }
        return false;
    }

    public static void makeSureNoValueDuplicate(List<String> aList,
                                                boolean caseSensitive) {
        if (null == aList || aList.isEmpty()) {
            throw new IllegalArgumentException("您提供的作业配置有误, List不能为空.");
        }

        if (1 == aList.size()) {
            return;
        } else {
            List<String> list = null;
            if (!caseSensitive) {
                list = valueToLowerCase(aList);
            } else {
                list = new ArrayList<String>(aList);
            }

            Collections.sort(list);

            for (int i = 0, len = list.size() - 1; i < len; i++) {
                if (list.get(i).equals(list.get(i + 1))) {
                    throw DataXException
                            .asDataXException(
                                    CommonErrorCode.CONFIG_ERROR,
                                    String.format(
                                            "您提供的作业配置信息有误, String:[%s] 不允许重复出现在列表中: [%s].",
                                            list.get(i),
                                            StringUtils.join(aList, ",")));
                }
            }
        }
    }

    public static boolean checkIfBInA(List<String> aList, List<String> bList,
                                      boolean caseSensitive) {
        if (null == aList || aList.isEmpty() || null == bList
                || bList.isEmpty()) {
            throw new IllegalArgumentException("您提供的作业配置有误, List不能为空.");
        }

        try {
            makeSureBInA(aList, bList, caseSensitive);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public static void makeSureBInA(List<String> aList, List<String> bList,
                                    boolean caseSensitive) {
        if (null == aList || aList.isEmpty() || null == bList
                || bList.isEmpty()) {
            throw new IllegalArgumentException("您提供的作业配置有误, List不能为空.");
        }

        List<String> all = null;
        List<String> part = null;

        if (!caseSensitive) {
            all = valueToLowerCase(aList);
            part = valueToLowerCase(bList);
        } else {
            all = new ArrayList<String>(aList);
            part = new ArrayList<String>(bList);
        }

        for (String oneValue : part) {
            if (!all.contains(oneValue)) {
                throw DataXException
                        .asDataXException(
                                CommonErrorCode.CONFIG_ERROR,
                                String.format(
                                        "您提供的作业配置信息有误, String:[%s] 不存在于列表中:[%s].",
                                        oneValue, StringUtils.join(aList, ",")));
            }
        }

    }

    public static boolean checkIfValueSame(List<Boolean> aList) {
        if (null == aList || aList.isEmpty()) {
            throw new IllegalArgumentException("您提供的作业配置有误, List不能为空.");
        }

        if (1 == aList.size()) {
            return true;
        } else {
            Boolean firstValue = aList.get(0);
            for (int i = 1, len = aList.size(); i < len; i++) {
                if (firstValue.booleanValue() != aList.get(i).booleanValue()) {
                    return false;
                }
            }
            return true;
        }
    }

    public static List<String> valueToLowerCase(List<String> aList) {
        if (null == aList || aList.isEmpty()) {
            throw new IllegalArgumentException("您提供的作业配置有误, List不能为空.");
        }
        List<String> result = new ArrayList<String>(aList.size());
        for (String oneValue : aList) {
            result.add(null != oneValue ? oneValue.toLowerCase() : null);
        }

        return result;
    }
    
    public static Boolean checkIfHasSameValue(List<String> listA, List<String> listB) {
        if (null == listA || listA.isEmpty() || null == listB || listB.isEmpty()) {
            return false;
        }

        for (String oneValue : listA) {
            if (listB.contains(oneValue)) {
                return true;
            }
        }

        return false;
    }
    
    public static boolean checkIfAllSameValue(List<String> listA, List<String> listB) {
        if (null == listA || listA.isEmpty() || null == listB || listB.isEmpty() || listA.size() != listB.size()) {
            return false;
        }
        return new HashSet<>(listA).containsAll(new HashSet<>(listB));
    }
}
