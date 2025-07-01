package com.alibaba.datax.plugin.writer.restwriter.handler.typenull;

import com.alibaba.datax.plugin.writer.restwriter.handler.TypeHandler;

/**
 * @author: zhangyongxiang
 * @date 2023/8/24 21:20
 **/

public class NullVoidTypeHandler implements TypeHandler<Object> {
    
    /**
     * unknown underlying type
     */
    @Override
    public Object convert(final Object object) {
        return null;
    }
}
