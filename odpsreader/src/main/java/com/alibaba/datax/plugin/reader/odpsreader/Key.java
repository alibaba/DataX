package com.alibaba.datax.plugin.reader.odpsreader;

public class Key {

    public final static String ACCESS_ID = "accessId";

    public final static String ACCESS_KEY = "accessKey";
    
    public final static String SECURITY_TOKEN = "securityToken";

    public static final String PROJECT = "project";

    public final static String TABLE = "table";

    public final static String PARTITION = "partition";

    public final static String ODPS_SERVER = "odpsServer";

    // 线上环境不需要填写，线下环境必填
    public final static String TUNNEL_SERVER = "tunnelServer";

    public final static String COLUMN = "column";

    // 当值为：partition 则只切分到分区；当值为：record，则当按照分区切分后达不到adviceNum时，继续按照record切分
    public final static String SPLIT_MODE = "splitMode";

    public final static String PACKAGE_AUTHORIZED_PROJECT = "packageAuthorizedProject";

    public final static String IS_COMPRESS = "isCompress";

    public final static String MAX_RETRY_TIME = "maxRetryTime";
    
    // 分区不存在时
    public final static String SUCCESS_ON_NO_PATITION="successOnNoPartition";

    // preSql
    public final static String PRE_SQL="preSql";

    // postSql
    public final static String POST_SQL="postSql";
    
}
