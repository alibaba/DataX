package com.alibaba.datax.plugin.writer.tablestorewriter.utils;

import net.sf.cglib.beans.BeanCopier;
import net.sf.cglib.core.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author daixinjie
 * @Description BeanCopier工具类
 * @Date 2019/3/26 17:56
 */
public class BeanCopierUtils {
    private static final Logger logger = LoggerFactory.getLogger(BeanCopierUtils.class);

    /**
     * BeanCopier缓存
     */
    public static Map<String, BeanCopier> beanCopierCacheMap = new HashMap<String, BeanCopier>();

    private static BasicTypeConverter basicTypeConverter = new BasicTypeConverter();

    /**
     * Java 基础包装类的Converter
     * boolean Boolean
     * byte	Byte
     * char	Character
     * short	Short
     * int	Integer
     * long	Long
     * float	Float
     * double	Double
     */
    static class BasicTypeConverter implements Converter {
        @Override
        public Object convert(Object value, Class target, Object context) {
            if (value instanceof Boolean) {
                return (Boolean) value;
            } else if (value instanceof Byte) {
                return (Byte) value;
            } else if (value instanceof Character) {
                return (Character) value;
            } else if (value instanceof Short) {
                return (Short) value;
            } else if (value instanceof Integer) {
                return (Integer) value;
            } else if (value instanceof Long) {
                return (Long) value;
            } else if (value instanceof Float) {
                return (Float) value;
            } else if (value instanceof Double) {
                return (Double) value;
            }
            return value;
        }
    }

    /**
     * 将source对象的属性拷贝到target对象中去
     *
     * @param source source对象
     * @param target target对象
     */
    public static void copyProperties(Object source, Object target) {
        String cacheKey = source.getClass().toString() +
                target.getClass().toString();

        BeanCopier beanCopier = null;

        if (!beanCopierCacheMap.containsKey(cacheKey)) {
            synchronized (BeanCopierUtils.class) {
                if (!beanCopierCacheMap.containsKey(cacheKey)) {
                    beanCopier = BeanCopier.create(source.getClass(), target.getClass(), true);
                    beanCopierCacheMap.put(cacheKey, beanCopier);
                } else {
                    beanCopier = beanCopierCacheMap.get(cacheKey);
                }
            }
        } else {
            beanCopier = beanCopierCacheMap.get(cacheKey);
        }

        beanCopier.copy(source, target, basicTypeConverter);
    }

    /**
     * 转换对象的类型
     *
     * @param source
     * @param destinationClass
     * @return
     */
    public static <T> T map(Object source, Class<T> destinationClass) {
        if (source == null) {
            return null;
        }
        T target = null;
        try {
            target = destinationClass.newInstance();
            copyProperties(source, target);
        } catch (Exception e) {
            logger.error("对象转换异常", e);
        }
        return target;
    }

    /**
     * 转换Collection中对象的类型
     *
     * @param sourceList
     * @param destinationClass
     * @return
     */
    public static <T, E> List<E> mapList(Collection<T> sourceList, Class<E> destinationClass) {
        if (sourceList == null) {
            return null;
        }
        List<E> destinationList = new ArrayList<E>();
        for (Object sourceObject : sourceList) {
            destinationList.add(map(sourceObject, destinationClass));
        }
        return destinationList;
    }

}
