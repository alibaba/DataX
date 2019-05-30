///**
// * 私有代码，未经许可，不得复制、散播；
// * 否则将可能依法追究责任。
// */
//
//package com.alibaba.datax.common.constant;
//
///**
// * Summary：<p></p>
// * Author : Martin
// * Since  : 2019/5/18 20:09
// */
//public enum JdbcDataType
//{
//    BIT("BIT", -7),
//
//    TINYINT("TINYINT", -6),
//
//    SMALLINT("SMALLINT", 5),
//
//    INTEGER("INTEGER", 4),
//
//    BIGINT("BIGINT", -5),
//
//    FLOAT("FLOAT", 6),
//
//    REAL("REAL", 7),
//
//    DOUBLE("DOUBLE", 8),
//
//    NUMERIC("NUMERIC", 2),
//
//    DECIMAL("DECIMAL", 3),
//
//    CHAR("CHAR", 1),
//
//    VARCHAR("VARCHAR", 12),
//
//    LONGVARCHAR("LONGVARCHAR", -1),
//
//    DATE("DATE", 91),
//
//    TIME("TIME", 92),
//
//    TIMESTAMP("TIMESTAMP", 93),
//
//    BINARY("BINARY", -2),
//
//    VARBINARY("VARBINARY", -3),
//
//    LONGVARBINARY("LONGVARBINARY", -4),
//
//    NULL("NULL", 0),
//
//    OTHER("OTHER", 1111),
//
//    JAVA_OBJECT("JAVA_OBJECT", 2000),
//
//    DISTINCT("DISTINCT", 2001),
//
//    STRUCT("STRUCT", 2002),
//
//    ARRAY("ARRAY", 2003),
//
//    BLOB("BLOB", 2004),
//
//    CLOB("CLOB", 2005),
//
//    REF("REF", 2006),
//
//    DATALINK("DATALINK", 70),
//
//    BOOLEAN("BOOLEAN", 16),
//
//    ROWID("ROWID", -8),
//
//    NCHAR("NCHAR", -15),
//
//    NVARCHAR("NVARCHAR", -9),
//
//    LONGNVARCHAR("LONGNVARCHAR", -16),
//
//    NCLOB("NCLOB", 2011),
//
//    SQLXML("SQLXML", 2009),
//
//    REF_CURSOR("REF_CURSOR", 2012),
//
//    TIME_WITH_TIMEZONE("TIME_WITH_TIMEZONE", 2013),
//
//    TIMESTAMP_WITH_TIMEZONE("TIMESTAMP_WITH_TIMEZONE", 2014);
//
//    private String jdbcType;
//    private int typeCode;
//
//    JdbcDataType(String jdbcType, int typeCode)
//    {
//        this.jdbcType = jdbcType;
//        this.typeCode = typeCode;
//    }
//
//    public int getTypeCode()
//    {
//        return typeCode;
//    }
//
//    public static JdbcDataType getByCode(final int code)
//    {
//        final JdbcDataType[] values = JdbcDataType.values();
//        for (JdbcDataType value : values) {
//            if (value.getTypeCode() == code) {
//                return value;
//            }
//        }
//        throw new IllegalArgumentException("Not Supported Type By Code[" + code + "].");
//    }
//
//    @Override
//    public String toString()
//    {
//        return "JdbcDataType{" +
//                "jdbcType='" + jdbcType + '\'' +
//                ", typeCode=" + typeCode +
//                '}';
//    }
//}
