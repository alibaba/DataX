package com.alibaba.datax.plugin.util;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

public class TypeUtil {

    private static final Set<Class> WrapClassSet = new HashSet(8);

    static {
        WrapClassSet.add(Byte.class);
        WrapClassSet.add(Short.class);
        WrapClassSet.add(Integer.class);
        WrapClassSet.add(Long.class);
        WrapClassSet.add(Float.class);
        WrapClassSet.add(Double.class);
        WrapClassSet.add(Boolean.class);
        WrapClassSet.add(Character.class);
    }

    /**
     * 判断值是否是特定基础类型，byte/Byte/short/Short/int/Integer/long/Long/float/Float/double/Double/boolean/Boolean
     *
     * @param obj 待判断的值
     * @param clz 包装类型的class
     * @return
     */
    public static boolean isPrimitive(Object obj, Class clz) {
        try {
            Field type = ((Class<?>) obj.getClass()).getField("TYPE");
            Class<?> aClass = (Class<?>) type.get(null);
            boolean aFlag = aClass.isPrimitive();
            Class<?> bClass = (Class<?>) clz.getField("TYPE").get(null);
            if (aFlag && WrapClassSet.contains(clz) && aClass == bClass) {
                return true;
            }
            return false;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 判断值是否是基础类型，byte/Byte/short/Short/int/Integer/long/Long/float/Float/double/Double/boolean/Boolean
     *
     * @param obj 待判断的值
     * @return
     */
    public static boolean isPrimitive(Object obj) {
        try {
            Field type = ((Class<?>) obj.getClass()).getField("TYPE");
            Class<?> aClass = (Class<?>) type.get(null);
            return aClass.isPrimitive();
        } catch (Exception e) {
            return false;
        }
    }
}
