

package com.alibaba.datax.common.element;

import com.alibaba.datax.common.constant.JavaDataType;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Summary：<p></p>
 * Author : Martin
 * Since  : 2019/5/18 20:01
 */
public abstract class ResourceColumn<T extends JavaDataType, U extends JDBCType>
{
    private T jType;
    private U jdbcDataType;
    private String dbBaseDataType;
    private Object rawData;
    private long byteSize;

    public ResourceColumn(T jType, U jdbcDataType, String dbBaseDataType, Object rawData, long byteSize)
    {
        this.jType = jType;
        this.jdbcDataType = jdbcDataType;
        this.dbBaseDataType = dbBaseDataType;
        this.rawData = rawData;
        this.byteSize = byteSize;
    }

    public T getjType()
    {
        return jType;
    }

    public U getJdbcDataType()
    {
        return jdbcDataType;
    }

    public String getDbBaseDataType()
    {
        return dbBaseDataType;
    }

    public Object getRawData()
    {
        return rawData;
    }

    public long getByteSize()
    {
        return byteSize;
    }

    public abstract byte[] asBytes();

    public abstract Short asShort();

    public abstract Integer asInteger();

    public abstract Long asLong();

    public abstract Float asFloat();

    public abstract Double asDouble();

    public abstract Character asCharacter();

    public abstract String asString();

    public abstract Boolean asBoolean();

    public abstract Date asDate();

    public abstract Time asTime();

    public abstract Timestamp asTimestamp();

    public abstract BigDecimal asBigdecimal();

    public abstract Array asArray();
}
