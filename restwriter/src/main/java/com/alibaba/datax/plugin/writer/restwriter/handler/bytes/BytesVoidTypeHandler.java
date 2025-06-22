package com.alibaba.datax.plugin.writer.restwriter.handler.bytes;

import com.alibaba.datax.plugin.writer.restwriter.handler.TypeHandler;

/**
 * @author: zhangyongxiang
 * @date 2023/8/24 21:48
 **/

public class BytesVoidTypeHandler implements TypeHandler<Object> {

    /**
     * underlying type is byte[]
     */
    @Override
    public Object convert(final Object object) {
        return object;
    }
}
