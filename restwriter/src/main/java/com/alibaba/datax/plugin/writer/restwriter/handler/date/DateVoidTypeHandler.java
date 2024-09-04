package com.alibaba.datax.plugin.writer.restwriter.handler.date;

import com.alibaba.datax.plugin.writer.restwriter.handler.TypeHandler;

/**
 * @author: zhangyongxiang
 * @date 2023/8/24 21:47
 **/

public class DateVoidTypeHandler implements TypeHandler<Object> {
    /**
     * underlying type is Long
     */
    @Override
    public Object convert(final Object object) {
        return object;
    }
}
