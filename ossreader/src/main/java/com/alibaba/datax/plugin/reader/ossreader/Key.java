package com.alibaba.datax.plugin.reader.ossreader;

/**
 * Created by mengxin.liumx on 2014/12/7.
 */
public class Key {
    public static final String ENDPOINT = "endpoint";

    public static final String ACCESSID = "accessId";

    public static final String ACCESSKEY = "accessKey";

    public static final String ENCODING = "encoding";

    public static final String BUCKET = "bucket";

    public static final String OBJECT = "object";
    
    public static final String CNAME = "cname";

    public static final String SUCCESS_ON_NO_Object = "successOnNoObject";

    public static final String PROXY_HOST = "proxyHost";

    public static final String PROXY_PORT = "proxyPort";

    public static final String PROXY_USERNAME = "proxyUsername";

    public static final String PROXY_PASSWORD = "proxyPassword";

    public static final String PROXY_DOMAIN = "proxyDomain";

    public static final String PROXY_WORKSTATION = "proxyWorkstation";

    public static final String HDOOP_CONFIG = "hadoopConfig";

    public static final String FS_OSS_ACCESSID = "fs.oss.accessKeyId";

    public static final String FS_OSS_ACCESSKEY = "fs.oss.accessKeySecret";

    public static final String FS_OSS_ENDPOINT = "fs.oss.endpoint";

    /*判断分片是否均匀的标准，是否有分片长度超出平均值的百分比*/
    public static final String BALANCE_THRESHOLD = "balanceThreshold";

}
