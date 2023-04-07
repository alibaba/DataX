package com.alibaba.datax.plugin.writer.odpswriter;


public final class Key {

    public final static String ODPS_SERVER = "odpsServer";

    public final static String TUNNEL_SERVER = "tunnelServer";

    public final static String ACCESS_ID = "accessId";

    public final static String ACCESS_KEY = "accessKey";

    public final static String SECURITY_TOKEN = "securityToken";

    public final static String PROJECT = "project";

    public final static String TABLE = "table";

    public final static String PARTITION = "partition";

    public final static String COLUMN = "column";

    public final static String TRUNCATE = "truncate";

    public final static String MAX_RETRY_TIME = "maxRetryTime";

    public final static String BLOCK_SIZE_IN_MB = "blockSizeInMB";

    //boolean 类型，default:false
    public final static String EMPTY_AS_NULL = "emptyAsNull";

    public final static String IS_COMPRESS = "isCompress";

    // preSql
    public final static String PRE_SQL="preSql";

    // postSql
    public final static String POST_SQL="postSql";
    
    public final static String CONSISTENCY_COMMIT = "consistencyCommit";
    
    public final static String UPLOAD_ID = "uploadId";
    
    public final static String TASK_COUNT = "taskCount";

    /**
     * support dynamic partition，支持动态分区，即根据读取到的record的某一列或几列来确定该record应该存入哪个分区
     * 1. 如何确定根据哪些列：根据目的表哪几列是分区列，再根据对应的column来路由
     * 2. 何时创建upload session：由于是动态分区，因此无法在初始化时确定分区，也就无法在初始化时创建 upload session，只有再读取到具体record之后才能创建
     * 3. 缓存 upload sesseion：每当出现新的分区，则创建新的session，同时将该分区对应的session缓存下来，以备下次又有需要存入该分区的记录
     * 4. 参数检查：不必要检查分区是否配置
     */
    public final static String SUPPORT_DYNAMIC_PARTITION = "supportDynamicPartition";

    /**
     * 动态分区下，用户如果将源表的某一个时间列映射到分区列，存在如下需求场景：源表的该时间列精确到秒，当时同步到odps表时，只想保留到天，并存入对应的天分区
     * 格式：
     * "partitionColumnMapping":[
     *      {
     *          "name":"pt", // 必填
     *          "srcDateFormat":"YYYY-MM-dd hh:mm:ss", // 可选，可能源表中的时间列是 String 类型，此时必须通过 fromDateFormat 来指定源表中该列的日期格式
     *          "dateFormat":"YYYY-MM-dd" // 必填
     *      },
     *      {
     *          ...
     *      },
     *
     *      ...
     * ]
     */
    public final static String PARTITION_COL_MAPPING = "partitionColumnMapping";
    public final static String PARTITION_COL_MAPPING_NAME = "name";
    public final static String PARTITION_COL_MAPPING_SRC_COL_DATEFORMAT = "srcDateFormat";
    public final static String PARTITION_COL_MAPPING_DATEFORMAT = "dateFormat";
    public final static String WRITE_TIMEOUT_IN_MS = "writeTimeoutInMs";

    public static final String OVER_LENGTH_RULE = "overLengthRule";
    //截断后保留的最大长度
    public static final String MAX_FIELD_LENGTH = "maxFieldLength";
    //odps本身支持的最大长度
    public static final String MAX_ODPS_FIELD_LENGTH = "maxOdpsFieldLength";
    public static final String ENABLE_OVER_LENGTH_OUTPUT = "enableOverLengthOutput";
    public static final String MAX_OVER_LENGTH_OUTPUT_COUNT = "maxOverLengthOutputCount";

    //动态分区写入模式下，内存使用率达到80%则flush时间间隔，单位分钟
    public static final String DYNAMIC_PARTITION_MEM_USAGE_FLUSH_INTERVAL_IN_MINUTE = "dynamicPartitionMemUsageFlushIntervalInMinute";
}
