package com.alibaba.datax.plugin.writer.adswriter.ads;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * ADS column data type.
 *
 * @since 0.0.1
 */
public class ColumnDataType {

    // public static final int NULL = 0;
    public static final int BOOLEAN = 1;
    public static final int BYTE = 2;
    public static final int SHORT = 3;
    public static final int INT = 4;
    public static final int LONG = 5;
    public static final int DECIMAL = 6;
    public static final int DOUBLE = 7;
    public static final int FLOAT = 8;
    public static final int TIME = 9;
    public static final int DATE = 10;
    public static final int TIMESTAMP = 11;
    public static final int STRING = 13;
    // public static final int STRING_IGNORECASE = 14;
    // public static final int STRING_FIXED = 21;

    public static final int MULTI_VALUE = 22;

    public static final int TYPE_COUNT = MULTI_VALUE + 1;

    /**
     * The list of types. An ArrayList so that Tomcat doesn't set it to null when clearing references.
     */
    private static final ArrayList<ColumnDataType> TYPES = new ArrayList<ColumnDataType>();
    private static final HashMap<String, ColumnDataType> TYPES_BY_NAME = new HashMap<String, ColumnDataType>();
    private static final ArrayList<ColumnDataType> TYPES_BY_VALUE_TYPE = new ArrayList<ColumnDataType>();

    /**
     * @param dataTypes
     * @return
     */
    public static String getNames(int[] dataTypes) {
        List<String> names = new ArrayList<String>(dataTypes.length);
        for (final int dataType : dataTypes) {
            names.add(ColumnDataType.getDataType(dataType).name);
        }
        return names.toString();
    }

    public int type;
    public String name;
    public int sqlType;
    public String jdbc;

    /**
     * How closely the data type maps to the corresponding JDBC SQL type (low is best).
     */
    public int sqlTypePos;

    static {
        for (int i = 0; i < TYPE_COUNT; i++) {
            TYPES_BY_VALUE_TYPE.add(null);
        }
        // add(NULL, Types.NULL, "Null", new String[] { "NULL" });
        add(STRING, Types.VARCHAR, "String", new String[] { "VARCHAR", "VARCHAR2", "NVARCHAR", "NVARCHAR2",
                "VARCHAR_CASESENSITIVE", "CHARACTER VARYING", "TID" });
        add(STRING, Types.LONGVARCHAR, "String", new String[] { "LONGVARCHAR", "LONGNVARCHAR" });
        // add(STRING_FIXED, Types.CHAR, "String", new String[] { "CHAR", "CHARACTER", "NCHAR" });
        // add(STRING_IGNORECASE, Types.VARCHAR, "String", new String[] { "VARCHAR_IGNORECASE" });
        add(BOOLEAN, Types.BOOLEAN, "Boolean", new String[] { "BOOLEAN", "BIT", "BOOL" });
        add(BYTE, Types.TINYINT, "Byte", new String[] { "TINYINT" });
        add(SHORT, Types.SMALLINT, "Short", new String[] { "SMALLINT", "YEAR", "INT2" });
        add(INT, Types.INTEGER, "Int", new String[] { "INTEGER", "INT", "MEDIUMINT", "INT4", "SIGNED" });
        add(INT, Types.INTEGER, "Int", new String[] { "SERIAL" });
        add(LONG, Types.BIGINT, "Long", new String[] { "BIGINT", "INT8", "LONG" });
        add(LONG, Types.BIGINT, "Long", new String[] { "IDENTITY", "BIGSERIAL" });
        add(DECIMAL, Types.DECIMAL, "BigDecimal", new String[] { "DECIMAL", "DEC" });
        add(DECIMAL, Types.NUMERIC, "BigDecimal", new String[] { "NUMERIC", "NUMBER" });
        add(FLOAT, Types.REAL, "Float", new String[] { "REAL", "FLOAT4" });
        add(DOUBLE, Types.DOUBLE, "Double", new String[] { "DOUBLE", "DOUBLE PRECISION" });
        add(DOUBLE, Types.FLOAT, "Double", new String[] { "FLOAT", "FLOAT8" });
        add(TIME, Types.TIME, "Time", new String[] { "TIME" });
        add(DATE, Types.DATE, "Date", new String[] { "DATE" });
        add(TIMESTAMP, Types.TIMESTAMP, "Timestamp", new String[] { "TIMESTAMP", "DATETIME", "SMALLDATETIME" });
        add(MULTI_VALUE, Types.VARCHAR, "String", new String[] { "MULTIVALUE" });
    }

    private static void add(int type, int sqlType, String jdbc, String[] names) {
        for (int i = 0; i < names.length; i++) {
            ColumnDataType dt = new ColumnDataType();
            dt.type = type;
            dt.sqlType = sqlType;
            dt.jdbc = jdbc;
            dt.name = names[i];
            for (ColumnDataType t2 : TYPES) {
                if (t2.sqlType == dt.sqlType) {
                    dt.sqlTypePos++;
                }
            }
            TYPES_BY_NAME.put(dt.name, dt);
            if (TYPES_BY_VALUE_TYPE.get(type) == null) {
                TYPES_BY_VALUE_TYPE.set(type, dt);
            }
            TYPES.add(dt);
        }
    }

    /**
     * Get the list of data types.
     * 
     * @return the list
     */
    public static ArrayList<ColumnDataType> getTypes() {
        return TYPES;
    }

    /**
     * Get the name of the Java class for the given value type.
     * 
     * @param type the value type
     * @return the class name
     */
    public static String getTypeClassName(int type) {
        switch (type) {
            case BOOLEAN:
                // "java.lang.Boolean";
                return Boolean.class.getName();
            case BYTE:
                // "java.lang.Byte";
                return Byte.class.getName();
            case SHORT:
                // "java.lang.Short";
                return Short.class.getName();
            case INT:
                // "java.lang.Integer";
                return Integer.class.getName();
            case LONG:
                // "java.lang.Long";
                return Long.class.getName();
            case DECIMAL:
                // "java.math.BigDecimal";
                return BigDecimal.class.getName();
            case TIME:
                // "java.sql.Time";
                return Time.class.getName();
            case DATE:
                // "java.sql.Date";
                return Date.class.getName();
            case TIMESTAMP:
                // "java.sql.Timestamp";
                return Timestamp.class.getName();
            case STRING:
                // case STRING_IGNORECASE:
                // case STRING_FIXED:
            case MULTI_VALUE:
                // "java.lang.String";
                return String.class.getName();
            case DOUBLE:
                // "java.lang.Double";
                return Double.class.getName();
            case FLOAT:
                // "java.lang.Float";
                return Float.class.getName();
                // case NULL:
                // return null;
            default:
                throw new IllegalArgumentException("type=" + type);
        }
    }

    /**
     * Get the data type object for the given value type.
     * 
     * @param type the value type
     * @return the data type object
     */
    public static ColumnDataType getDataType(int type) {
        if (type < 0 || type >= TYPE_COUNT) {
            throw new IllegalArgumentException("type=" + type);
        }
        ColumnDataType dt = TYPES_BY_VALUE_TYPE.get(type);
        // if (dt == null) {
        // dt = TYPES_BY_VALUE_TYPE.get(NULL);
        // }
        return dt;
    }

    /**
     * Convert a value type to a SQL type.
     * 
     * @param type the value type
     * @return the SQL type
     */
    public static int convertTypeToSQLType(int type) {
        return getDataType(type).sqlType;
    }

    /**
     * Convert a SQL type to a value type.
     * 
     * @param sqlType the SQL type
     * @return the value type
     */
    public static int convertSQLTypeToValueType(int sqlType) {
        switch (sqlType) {
        // case Types.CHAR:
        // case Types.NCHAR:
        // return STRING_FIXED;
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                return STRING;
            case Types.NUMERIC:
            case Types.DECIMAL:
                return DECIMAL;
            case Types.BIT:
            case Types.BOOLEAN:
                return BOOLEAN;
            case Types.INTEGER:
                return INT;
            case Types.SMALLINT:
                return SHORT;
            case Types.TINYINT:
                return BYTE;
            case Types.BIGINT:
                return LONG;
            case Types.REAL:
                return FLOAT;
            case Types.DOUBLE:
            case Types.FLOAT:
                return DOUBLE;
            case Types.DATE:
                return DATE;
            case Types.TIME:
                return TIME;
            case Types.TIMESTAMP:
                return TIMESTAMP;
                // case Types.NULL:
                // return NULL;
            default:
                throw new IllegalArgumentException("JDBC Type: " + sqlType);
        }
    }

    /**
     * Get the value type for the given Java class.
     * 
     * @param x the Java class
     * @return the value type
     */
    public static int getTypeFromClass(Class<?> x) {
        // if (x == null || Void.TYPE == x) {
        // return NULL;
        // }
        if (x.isPrimitive()) {
            x = getNonPrimitiveClass(x);
        }
        if (String.class == x) {
            return STRING;
        } else if (Integer.class == x) {
            return INT;
        } else if (Long.class == x) {
            return LONG;
        } else if (Boolean.class == x) {
            return BOOLEAN;
        } else if (Double.class == x) {
            return DOUBLE;
        } else if (Byte.class == x) {
            return BYTE;
        } else if (Short.class == x) {
            return SHORT;
        } else if (Float.class == x) {
            return FLOAT;
            // } else if (Void.class == x) {
            // return NULL;
        } else if (BigDecimal.class.isAssignableFrom(x)) {
            return DECIMAL;
        } else if (Date.class.isAssignableFrom(x)) {
            return DATE;
        } else if (Time.class.isAssignableFrom(x)) {
            return TIME;
        } else if (Timestamp.class.isAssignableFrom(x)) {
            return TIMESTAMP;
        } else if (java.util.Date.class.isAssignableFrom(x)) {
            return TIMESTAMP;
        } else {
            throw new IllegalArgumentException("class=" + x);
        }
    }

    /**
     * Convert primitive class names to java.lang.* class names.
     * 
     * @param clazz the class (for example: int)
     * @return the non-primitive class (for example: java.lang.Integer)
     */
    public static Class<?> getNonPrimitiveClass(Class<?> clazz) {
        if (!clazz.isPrimitive()) {
            return clazz;
        } else if (clazz == boolean.class) {
            return Boolean.class;
        } else if (clazz == byte.class) {
            return Byte.class;
        } else if (clazz == char.class) {
            return Character.class;
        } else if (clazz == double.class) {
            return Double.class;
        } else if (clazz == float.class) {
            return Float.class;
        } else if (clazz == int.class) {
            return Integer.class;
        } else if (clazz == long.class) {
            return Long.class;
        } else if (clazz == short.class) {
            return Short.class;
        } else if (clazz == void.class) {
            return Void.class;
        }
        return clazz;
    }

    /**
     * Get a data type object from a type name.
     * 
     * @param s the type name
     * @return the data type object
     */
    public static ColumnDataType getTypeByName(String s) {
        return TYPES_BY_NAME.get(s);
    }

    /**
     * Check if the given value type is a String (VARCHAR,...).
     * 
     * @param type the value type
     * @return true if the value type is a String type
     */
    public static boolean isStringType(int type) {
        if (type == STRING /* || type == STRING_FIXED || type == STRING_IGNORECASE */
                || type == MULTI_VALUE) {
            return true;
        }
        return false;
    }

    /**
     * @return
     */
    public boolean supportsAdd() {
        return supportsAdd(type);
    }

    /**
     * Check if the given value type supports the add operation.
     * 
     * @param type the value type
     * @return true if add is supported
     */
    public static boolean supportsAdd(int type) {
        switch (type) {
            case BYTE:
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
            case INT:
            case LONG:
            case SHORT:
                return true;
            default:
                return false;
        }
    }

    /**
     * Get the data type that will not overflow when calling 'add' 2 billion times.
     * 
     * @param type the value type
     * @return the data type that supports adding
     */
    public static int getAddProofType(int type) {
        switch (type) {
            case BYTE:
                return LONG;
            case FLOAT:
                return DOUBLE;
            case INT:
                return LONG;
            case LONG:
                return DECIMAL;
            case SHORT:
                return LONG;
            default:
                return type;
        }
    }

}
