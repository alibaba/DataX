package com.alibaba.datax.plugin.writer.obswriter;

public class Key {

    /**
     * endpoint: OBS所在地区
     */
    public static final String ENDPOINT = "endpoint";

    public static final String ACCESSKEY = "accessKey";

    public static final String SECRETKEY = "secretKey";

    /**
     * bucket: OBS桶名称
     */
    public static final String BUCKET = "bucket";

    /**
     * object: OBS项目名称
     */
    public static final String OBJECT = "object";
    
    public static final String CNAME = "cname";

    public static final String PARTITION = "partition";

    /**
     * encrypt: 是否需要将数据在obs上加密存储
     */
    public static final String ENCRYPT = "encrypt";

    public static final String BLOCK_SIZE_IN_MB = "blockSizeInMB";

    public static final String OBS_CONFIG = "obs";

    public static final String POSTGRESQL_CONFIG = "postgresql";

    public static final String PROXY_HOST = "proxyHost";

    public static final String PROXY_PORT = "proxyPort";

    public static final String PROXY_USERNAME = "proxyUsername";

    public static final String PROXY_PASSWORD = "proxyPassword";

    public static final String PROXY_DOMAIN = "proxyDomain";

    public static final String PROXY_WORKSTATION = "proxyWorkstation";

    public static final String HDOOP_CONFIG = "hadoopConfig";

    public static final String FS_OBS_ACCESSKEY = "fs.obs.accessKey";

    public static final String FS_OBS_SECRETKEY = "fs.obs.secretKey";

    public static final String FS_OBS_ENDPOINT = "fs.obs.endpoint";

    /**
     * 多个task是否写单个object文件：
     * false 多个task写多个object（默认是false, 保持向前兼容）
     * true 多个task写单个object
     */
    public static final String WRITE_SINGLE_OBJECT = "writeSingleObject";

    public static final String UPLOAD_ID = "uploadId";

    /**
     * Only for parquet or orc fileType
     */
    public static final String PATH = "path";
    /**
     * Only for parquet or orc fileType
     */
    public static final String FILE_NAME = "fileName";

    public static final String GENERATE_EMPTY_FILE = "generateEmptyFile";


}
