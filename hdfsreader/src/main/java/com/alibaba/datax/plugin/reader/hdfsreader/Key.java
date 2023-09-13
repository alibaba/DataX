package com.alibaba.datax.plugin.reader.hdfsreader;

public final class Key {

    /**
     * 此处声明插件用到的需要插件使用者提供的配置项
     */
    public final static String PATH = "path";
    public final static String DEFAULT_FS = "defaultFS";
    public final static String HIVE_VERSION = "hiveVersion";
    public static final String FILETYPE = "fileType";
    public static final String HADOOP_CONFIG = "hadoopConfig";
    public static final String HAVE_KERBEROS = "haveKerberos";
    public static final String KERBEROS_KEYTAB_FILE_PATH = "kerberosKeytabFilePath";
    public static final String KERBEROS_CONF_FILE_PATH = "kerberosConfFilePath";
    public static final String KERBEROS_PRINCIPAL = "kerberosPrincipal";
    public static final String PATH_FILTER = "pathFilter";
    public static final String PARQUET_SCHEMA = "parquetSchema";
    /**
     * hive 3.x 或 cdh高版本，使用UTC时区存储时间戳，如果发现时区偏移，该配置项要配置成 true
     */
    public static final String PARQUET_UTC_TIMESTAMP = "parquetUtcTimestamp";
    public static final String SUCCESS_ON_NO_FILE = "successOnNoFile";
    public static final String PROTECTION = "protection";

    /**
     * 用于显示地指定hdfs客户端的用户名
     */
    public static final String HDFS_USERNAME = "hdfsUsername";

    /**
     * ORC FILE空文件大小
     */
    public static final String ORCFILE_EMPTYSIZE = "orcFileEmptySize";

    /**
     * 是否跳过空的OrcFile
     */
    public static final String SKIP_EMPTY_ORCFILE = "skipEmptyOrcFile";

    /**
     * 是否跳过 orc meta 信息
     */

    public static final String SKIP_ORC_META = "skipOrcMetaInfo";
    /**
     * 过滤_或者.开头的文件
     */
    public static final String REGEX_PATTERN = "^.*[/][^._].*";

    public static final String FILTER_TAG_FILE = "filterTagFile";

    // high level params refs https://github.com/aliyun/alibabacloud-jindodata/blob/master/docs/user/4.x/4.4.0/oss/configuration/jindosdk_configuration_list.md
    // <!-- oss 并发下载任务队列大小 -->
    public static final String FS_OSS_DOWNLOAD_QUEUE_SIZE = "ossDownloadQueueSize";

    //    <!-- 进程内 oss 最大并发下载任务数 -->
    public static final String FS_OSS_DOWNLOAD_THREAD_CONCURRENCY = "ossDownloadThreadConcurrency";

    public static final String FS_OSS_READ_READAHEAD_BUFFER_COUNT = "ossDownloadBufferCount";

    public static final String FILE_SYSTEM_TYPE = "fileSystemType";
    public static final String CDH_3_X_HIVE_VERSION = "3.1.3-cdh";

    public static final String SUPPORT_ADD_MIDDLE_COLUMN = "supportAddMiddleColumn";
}
