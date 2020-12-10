package com.alibaba.datax.plugin.rdbms.writer;

/**
 * 用于插件解析用户配置时，需要进行标识（MARK）的常量的声明.
 */
public final class Constant {
    public static final int DEFAULT_BATCH_SIZE = 2048;

    public static final int DEFAULT_BATCH_BYTE_SIZE = 32 * 1024 * 1024;

    public static String TABLE_NAME_PLACEHOLDER = "@table";

    public static String CONN_MARK = "connection";

    public static String TABLE_NUMBER_MARK = "tableNumber";

    public static String INSERT_OR_REPLACE_TEMPLATE_MARK = "insertOrReplaceTemplate";

    public static final String OB10_SPLIT_STRING = "||_dsc_ob10_dsc_||";
    public static final String OB10_SPLIT_STRING_PATTERN = "\\|\\|_dsc_ob10_dsc_\\|\\|";

}
