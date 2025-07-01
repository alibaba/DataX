package com.alibaba.datax.plugin.writer.restwriter.handler.typedouble;

import com.alibaba.datax.plugin.writer.restwriter.handler.TypeHandler;

/**
 * @author: zhangyongxiang
 * @date 2023/8/24 21:45
 **/

public class DoubleVoidTypeHandler implements TypeHandler<Object> {
    /**
     * underlying type is Double
     */
    @Override
    public Object convert(final Object object) {
        return object;
    }
}
