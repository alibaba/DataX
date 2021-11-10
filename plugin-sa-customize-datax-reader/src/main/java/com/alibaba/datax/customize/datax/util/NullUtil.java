package com.alibaba.datax.customize.datax.util;
import java.util.Objects;

public class NullUtil {

    public static boolean isNullOrBlank(Object value) {
        if (Objects.isNull(value) || ((value instanceof String) && "".equals(value))) {
            return true;
        }
        return false;
    }
}
