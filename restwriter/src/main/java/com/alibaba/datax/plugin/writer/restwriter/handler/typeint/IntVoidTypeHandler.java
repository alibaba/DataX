package com.alibaba.datax.plugin.writer.restwriter.handler.typeint;

import com.alibaba.datax.plugin.writer.restwriter.handler.TypeHandler;

/**
 * @author: zhangyongxiang
 * @date 2023/8/24 21:22
 **/

public class IntVoidTypeHandler implements TypeHandler<Object> {

    /**
     * underlying type is BigInteger
     */

    @Override
    public Object convert(final Object object) {
        return object;
    }
}
