package com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils;

import com.alibaba.datax.common.util.Configuration;

public class ParamChecker {

    private static void throwNotExistException() {
        throw new IllegalArgumentException("missing the key.");
    }

    private static void throwStringLengthZeroException() {
        throw new IllegalArgumentException("input the key is empty string.");
    }

    public static String checkStringAndGet(Configuration param, String key, boolean isTrim) {
        try {
            String value = param.getString(key);
            if (isTrim) {
                value = value != null ? value.trim() : null;
            }
            if (null == value) {
                throwNotExistException();
            } else if (value.length() == 0) {
                throwStringLengthZeroException();
            }
            return value;
        } catch(RuntimeException e) {
            throw e;
        }
    }
}
