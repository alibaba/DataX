

package com.alibaba.datax.common.constant;

/**
 * Summary：<p></p>
 * Author : Martin
 * Since  : 2019/5/30 20:34
 */
public enum PostgresqlDataType
{
    /**
     * bit  -> Boolean
     * bool  -> Boolean
     * box  -> PGbox
     * bpchar  -> String
     * bytea -> Object
     * cidr  -> Object
     * circle  -> PGcircle
     * date  -> Date
     * float4  -> Float
     * float8  -> Double
     * inet  -> Object
     * int2  -> Integer
     * int4  -> Integer
     * int8  -> Long
     * interval  -> PGInterval
     * lseg  -> PGlseg
     * macaddr  -> Object
     * numeric  -> BigDecimal
     * path  -> PGpath
     * point  -> PGpoint 
     * polygon  -> PGpolygon 
     * text  -> String
     * time  -> Time
     * timestamp  -> Timestamp
     * timestamptz  -> Timestamp
     * timetz  -> Time
     * varbit  -> Object
     * varchar  -> String
     */
    BIT("bit"),
    BOOL("bool"),
    BOX("box"),
    BPCHAR("bpchar"),
    BYTEA("bytea"),
    CIDR("cidr"),
    CIRCLE("circle"),
    DATE("date"),
    FLOAT4("float4"),
    FLOAT8("float8"),
    INET("inet"),
    INT2("int2"),
    INT4("int4"),
    INT8("int8"),
    INTERVAL("interval"),
    LSEG("lseg"),
    MACADDR("macaddr"),
    NUMERIC("numeric"),
    PATH("path"),
    POINT("point"),
    POLYGON("polygon"),
    TEXT("text"),
    TIME("time"),
    TIMESTAMP("timestamp"),
    TIMESTAMPTZ("timestamptz"),
    TIMETZ("timetz"),
    VARBIT("varbit"),
    VARCHAR("varchar"),
    NULL("null");
    private String typeName;

    PostgresqlDataType(String typeName)
    {
        this.typeName = typeName;
    }

    public String getTypeName()
    {
        return typeName;
    }

    public static PostgresqlDataType of(final String datatype)
    {
        final PostgresqlDataType[] values = PostgresqlDataType.values();
        for (PostgresqlDataType value : values) {
            final String typeName = value.getTypeName();
            if (typeName.equalsIgnoreCase(datatype) || ("_" + typeName).equalsIgnoreCase(datatype)) {
                return value;
            }
        }
        throw new IllegalArgumentException("Not supported PGDataType[" + datatype + "]");
    }
}
