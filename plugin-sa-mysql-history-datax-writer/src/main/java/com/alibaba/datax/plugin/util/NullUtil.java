package com.alibaba.datax.plugin.util;

import cn.hutool.core.util.StrUtil;

import java.util.Objects;

public class NullUtil {

    public static boolean isNullOrBlank(Object value) {
        if (Objects.isNull(value) || ((value instanceof String) && StrUtil.isBlank((String) value))) {
            return true;
        }
        return false;
    }
}
