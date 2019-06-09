

package com.alibaba.datax.common.util;

import com.alibaba.datax.common.constant.JavaDataType;
import com.alibaba.datax.common.constant.DataSourceType;
import com.alibaba.datax.common.constant.PostgresqlDataType;

import java.sql.JDBCType;

/**
 * Summary：<p></p>
 * Author : Martin
 * Since  : 2019/5/19 9:03
 */
public class DataTypeMapper
{
    public static JDBCType parsePGDatatypeToJdbc(PostgresqlDataType pgDataType)
    {
        switch (pgDataType) {
            case BIT:
            case BOOL:
                return JDBCType.BIT;
            case DATE:
                return JDBCType.DATE;
            case FLOAT8:
                return JDBCType.DOUBLE;
            case FLOAT4:
                return JDBCType.REAL;
            case INT2:
            case INT4:
                return JDBCType.INTEGER;
            case INT8:
                return JDBCType.BIGINT;
            case BPCHAR:
            case VARCHAR:
                return JDBCType.VARCHAR;
            case TEXT:
                return JDBCType.LONGVARCHAR;
            case TIME:
            case TIMETZ:
                return JDBCType.TIME;
            case TIMESTAMP:
            case TIMESTAMPTZ:
                return JDBCType.TIMESTAMP;
            case BYTEA:
            case CIDR:
            case INET:
            case MACADDR:
            case VARBIT:
                return JDBCType.JAVA_OBJECT;
            case INTERVAL:
            case BOX:
            case CIRCLE:
            case LSEG:
            case PATH:
            case POINT:
            case POLYGON:
                return JDBCType.OTHER;
            default:
                throw new IllegalArgumentException("Not supported PGDataType[" + pgDataType + "].");
        }
    }

    public <T extends DataSourceType, V> V parseToDataSourceDataType(final T dataSourceType
            , final JavaDataType javaDataType)
    {
        return null;
    }

    public static JavaDataType parseJdbcDataTypeToJava(final int jdbcDataType)
    {
        final JDBCType jdbcType = JDBCType.valueOf(jdbcDataType);
        return parseJdbcDataTypeToJava(jdbcType);
    }

    public static JavaDataType parseJdbcDataTypeToJava(final JDBCType jdbcDataType)
    {
        switch (jdbcDataType) {
            case CHAR:
            case VARCHAR:
            case LONGVARCHAR:
            case NCHAR: // new
            case NVARCHAR: // new
            case LONGNVARCHAR: // new
            case CLOB: // new
            case NCLOB: // new
                return JavaDataType.STRING;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return JavaDataType.INTEGER;
            case BIGINT:
                return JavaDataType.LONG;
            case REAL:
                return JavaDataType.FLOAT;
            case NUMERIC:
            case DECIMAL:
                return JavaDataType.BIGDECIMAL;
            case FLOAT:
            case DOUBLE:
                return JavaDataType.DOUBLE;
            case DATE:
                return JavaDataType.DATE;
            case TIME:
                return JavaDataType.TIME;
            case TIMESTAMP:
                return JavaDataType.TIMESTAMP;
            case BLOB: // new
            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
                return JavaDataType.BYTE;
            case BIT:
            case BOOLEAN: // new
                return JavaDataType.BOOLEAN;
            case ARRAY:
                return JavaDataType.ARRAY;
            case NULL:
                return JavaDataType.NULL;
            default:
                return JavaDataType.BADTYPE;
        }
    }
}
