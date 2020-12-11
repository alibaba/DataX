package com.alibaba.datax.plugin.rdbms.reader;

public final class Constant {
    public static final String PK_TYPE = "pkType";

    public static final Object PK_TYPE_STRING = "pkTypeString";

    public static final Object PK_TYPE_LONG = "pkTypeLong";
    
    public static final Object PK_TYPE_MONTECARLO = "pkTypeMonteCarlo";
    
    public static final String SPLIT_MODE_RANDOMSAMPLE = "randomSampling";

    public static String CONN_MARK = "connection";

    public static String TABLE_NUMBER_MARK = "tableNumber";

    public static String IS_TABLE_MODE = "isTableMode";

    public final static String FETCH_SIZE = "fetchSize";

    public static String QUERY_SQL_TEMPLATE_WITHOUT_WHERE = "select %s from %s ";

    public static String QUERY_SQL_TEMPLATE = "select %s from %s where (%s)";

    public static String TABLE_NAME_PLACEHOLDER = "@table";

    public static Integer SPLIT_FACTOR = 5;

}
