package com.alibaba.datax.plugin.reader.otsreader.utils;

import com.alibaba.datax.common.util.Configuration;

import java.util.List;

public class ParamCheckerOld {

    private static void throwNotExistException(String key) {
        throw new IllegalArgumentException("The param '" + key + "' is not exist.");
    }

    private static void throwEmptyException(String key) {
        throw new IllegalArgumentException("The param  '" + key + "' is empty.");
    }

    private static void throwNotListException(String key) {
        throw new IllegalArgumentException("The param  '" + key + "' is not a json array.");
    }

    public static List<Object> checkListAndGet(Configuration param, String key, boolean isCheckEmpty) {
        List<Object> value = null;
        try {
            value = param.getList(key);
        } catch (ClassCastException e) {
            throwNotListException(key);
        }
        if (null == value) {
            throwNotExistException(key);
        } else if (isCheckEmpty && value.isEmpty()) {
            throwEmptyException(key);
        }
        return value;
    }

}
