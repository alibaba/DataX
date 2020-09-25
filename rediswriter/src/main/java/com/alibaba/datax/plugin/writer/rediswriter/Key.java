package com.alibaba.datax.plugin.writer.rediswriter;

public class Key {
    public final static String REDISMODE = "redisMode";
    public final static String ADDRESS = "address";
    public final static String AUTH = "auth";

    public final static String BATCH_SIZE = "batchSize";

    //对应redis的数据类型，目前支持三种，string，list，hash
    public final static String WRITE_TYPE = "writeType";

    //对应redis key值的hive列配置
    public final static String COLKEY = "colKey";

    //数据库列名
    public final static String COL_NAME = "name";
    //数据库列索引
    public final static String COL_INDEX = "index";

    //对应redis value值的hive列
    public final static String COLVALUE = "colValue";
    //redis key值的前缀，非必须
    public final static String KEY_PREFIX = "keyPrefix";

    //redis key值的后缀，非必须
    public final static String KEY_SUFFIX = "keySuffix";

    //redis value值的前缀，非必须
    public final static String VALUE_PREFIX  = "valuePrefix";
    //redis value值的后缀，非必须
    public final static String VALUE_SUFFIX = "valueSuffix";

    //自定义的redis key值，非hive列
    public final static String STRING_KEY = "strKey";

    // redis key值的过期时间,单位秒
    public final static String EXPIRE = "expire";

    // 默认是insert，也可以delete redis
    public final static String WRITE_MODE = "writeMode";
    // 具体每种类型配置
    public final static String CONFIG = "config";
    // redis list类型的push类型，有lpush，rpush，overwrite
    public final static String LIST_PUSH_TYPE = "pushType";

    // redis list类型对应数据源column值的分隔符,只支持string类型的数据源column
    public final static String LIST_VALUE_DELIMITER = "valueDelimiter";

    // hash类型要删除的field，次参数只对删除hash类型的field时有效
    public final static String HASH_DELETE_FILEDS = "hashFields";

}
