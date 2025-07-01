package com.alibaba.datax.plugin.writer.restwriter.handler.string;

import com.alibaba.datax.plugin.writer.restwriter.handler.TypeHandler;

/**
 * @author: zhangyongxiang
 * @date 2023/8/24 21:46
 **/
public class StringVoidTypeHandler implements TypeHandler<Object> {
    
    /**
     * underlying type is String
     */
    @Override
    public Object convert(final Object object) {
        return object;
    }
}
