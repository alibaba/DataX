

package com.alibaba.datax.common.element;

import com.alibaba.fastjson.JSON;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

/**
 * Summary：<p></p>
 * Author : Martin
 * Since  : 2019/6/10 2:46
 */
public class JavaObjColumn
        extends Column
{

    public JavaObjColumn(Object rawData)
    {
        super(rawData, Type.JAVA_OBJECT, (rawData == null) ? 0 : JSON.toJSONString(rawData).length());
    }

    public JavaObjColumn(Object rawData, Type type, int byteSize)
    {
        super(rawData, type, byteSize);
    }

    @Override
    public Long asLong()
    {
        return null;
    }

    @Override
    public Double asDouble()
    {
        return null;
    }

    @Override
    public String asString()
    {
        return null;
    }

    @Override
    public Date asDate()
    {
        return null;
    }

    @Override
    public byte[] asBytes()
    {
        return new byte[0];
    }

    @Override
    public Boolean asBoolean()
    {
        return null;
    }

    @Override
    public BigDecimal asBigDecimal()
    {
        return null;
    }

    @Override
    public BigInteger asBigInteger()
    {
        return null;
    }
}
