package com.alibaba.datax.plugin.reader.kudureader;

/**
 * @author daizihao
 * @create 2021-01-19 15:18
 **/
public class Key {
    public final static String KUDU_CONFIG = "kuduConfig";
    public final static String KUDU_MASTER = "kudu.master_addresses";
    public final static String KUDU_ADMIN_TIMEOUT = "timeout";
    public final static String KUDU_SESSION_TIMEOUT = "sessionTimeout";

    public final static String TABLE = "table";
    public final static String COLUMN = "column";
    public static final String NAME = "name";
    public static final String TYPE = "type";
    public static final String VALUE = "value";
    public static final String FORMAT = "format";

    public final static String SPLIT_PK = "splitPk";
    public final static String SPLIT_PK_UPPER = "splitPkUpper";
    public final static String SPLIT_PK_LOWER = "splitPkLower";
}
