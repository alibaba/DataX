package com.alibaba.datax.plugin.writer.restwriter.handler.bool;

import com.alibaba.datax.plugin.writer.restwriter.handler.TypeHandler;

/**
 * @author: zhangyongxiang
 * @date 2023/8/24 21:46
 **/
public class BoolVoidTypeHandler implements TypeHandler<Object> {
    /**
     * underlying type is Boolean
     */
    @Override
    public Object convert(final Object object) {
        return object;
    }
}
