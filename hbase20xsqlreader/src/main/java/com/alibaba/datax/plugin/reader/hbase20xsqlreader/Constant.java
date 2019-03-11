package com.alibaba.datax.plugin.reader.hbase20xsqlreader;

public class Constant {
    public static final String PK_TYPE = "pkType";

    public static final Object PK_TYPE_STRING = "pkTypeString";

    public static final Object PK_TYPE_LONG = "pkTypeLong";

    public static final String DEFAULT_SERIALIZATION = "PROTOBUF";

    public static final String CONNECT_STRING_TEMPLATE = "jdbc:phoenix:thin:url=%s;serialization=%s";

    public static final String CONNECT_DRIVER_STRING = "org.apache.phoenix.queryserver.client.Driver";

    public static final String SELECT_COLUMNS_TEMPLATE = "SELECT COLUMN_NAME, COLUMN_FAMILY FROM SYSTEM.CATALOG WHERE TABLE_NAME='%s' AND COLUMN_NAME IS NOT NULL";

    public static String QUERY_SQL_TEMPLATE_WITHOUT_WHERE = "select %s from %s ";

    public static String QUERY_SQL_TEMPLATE = "select %s from %s where (%s)";

    public static String QUERY_MIN_MAX_TEMPLATE = "SELECT MIN(%s),MAX(%s) FROM %s";

    public static String QUERY_COLUMN_TYPE_TEMPLATE = "SELECT %s FROM %s LIMIT 1";

    public static String QUERY_SQL_PER_SPLIT = "querySqlPerSplit";

}
