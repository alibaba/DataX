package com.alibaba.datax.plugin.writer.hdfswriter;

/**
 * Created by shf on 15/10/8.
 */
public class Key {
    // must have
    public static final String PATH = "path";
    //must have
    public final static String DEFAULT_FS = "defaultFS";
    //must have
    public final static String FILE_TYPE = "fileType";
    // must have
    public static final String FILE_NAME = "fileName";
    // must have for column
    public static final String COLUMN = "column";
    public static final String NAME = "name";
    public static final String TYPE = "type";
    public static final String DATE_FORMAT = "dateFormat";
    // must have
    public static final String WRITE_MODE = "writeMode";
    // must have
    public static final String FIELD_DELIMITER = "fieldDelimiter";
    // not must, default UTF-8
    public static final String ENCODING = "encoding";
    // not must, default no compress
    public static final String COMPRESS = "compress";
    // not must, not default \N
    public static final String NULL_FORMAT = "nullFormat";
    // Kerberos
    public static final String HAVE_KERBEROS = "haveKerberos";
    public static final String KERBEROS_KEYTAB_FILE_PATH = "kerberosKeytabFilePath";
    public static final String KERBEROS_PRINCIPAL = "kerberosPrincipal";
    // hadoop config
    public static final String HADOOP_CONFIG = "hadoopConfig";

    // useOldRawDataTransf
    public final static String PARQUET_FILE_USE_RAW_DATA_TRANSF = "useRawDataTransf";

    public final static String DATAX_PARQUET_MODE = "dataxParquetMode";

    // hdfs username 默认值 admin
    public final static String HDFS_USERNAME = "hdfsUsername";

    public static final String PROTECTION = "protection";

    public static final String PARQUET_SCHEMA = "parquetSchema";
    public static final String PARQUET_MERGE_RESULT = "parquetMergeResult";

    /**
     * hive 3.x 或 cdh高版本，使用UTC时区存储时间戳，如果发现时区偏移，该配置项要配置成 true
     */
    public static final String PARQUET_UTC_TIMESTAMP = "parquetUtcTimestamp";

    // Kerberos
    public static final String KERBEROS_CONF_FILE_PATH = "kerberosConfFilePath";

    // PanguFS
    public final static String PANGU_FS_CONFIG = "panguFSConfig";
    public final static String PANGU_FS_CONFIG_NUWA_CLUSTER = "nuwaCluster";
    public final static String PANGU_FS_CONFIG_NUWA_SERVERS = "nuwaServers";
    public final static String PANGU_FS_CONFIG_NUWA_PROXIES = "nuwaProxies";
    public final static String PANGU_FS_CONFIG_CAPABILITY = "capability";


    public static final String FS_OSS_UPLOAD_THREAD_CONCURRENCY = "ossUploadConcurrency";
    //  <!-- oss 并发上传任务队列大小 -->
    public static final String FS_OSS_UPLOAD_QUEUE_SIZE = "ossUploadQueueSize";
    //    <!-- 进程内 oss 最大并发上传任务数 -->
    public static final String FS_OSS_UPLOAD_MAX_PENDING_TASKS_PER_STREAM = "ossUploadMaxPendingTasksPerStream";

    public static final String FS_OSS_BLOCKLET_SIZE_MB = "ossBlockSize";

    public static final String FILE_SYSTEM_TYPE = "fileSystemType";
    public static final String ENABLE_COLUMN_EXCHANGE = "enableColumnExchange";
    public static final String SUPPORT_HIVE_DATETIME = "supportHiveDateTime";
}
