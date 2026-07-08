package com.alibaba.datax.plugin.writer.restwriter.handler;

/**
 * @author: zhangyongxiang
 * @date 2023/8/24 21:03
 **/
public interface TypeHandler<T> {
    T convert(Object object);
}
