package com.alibaba.datax.plugin.writer.restwriter.handler.typelong;

import com.alibaba.datax.plugin.writer.restwriter.handler.TypeHandler;

/**
 * @author: zhangyongxiang
 * @date 2023/8/24 21:23
 **/
public class LongVoidTypeHandler implements TypeHandler<Object> {

    /**
     * underlying type is BigInteger
     */
    @Override
    public Object convert(final Object object) {
        return object;
    }
}
