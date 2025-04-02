package com.alibaba.datax.plugin.writer.paimonwriter;

public class Key {

    public static final String PAIMON_DB_NAME = "databaseName";
    public static final String PAIMON_TABLE_NAME = "tableName";
    public static final String PAIMON_PRIMARY_KEY = "primaryKey";
    public static final String PAIMON_PARTITION_FIELDS = "partitionFields";
    public static final String PAIMON_BATCH_SIZE = "batchSize";

    public static final String PAIMON_COLUMN = "column";

    /**
     * writerOption
     */
    public static final String PAIMON_WRITE_OPTION = "writeOption";
    public static final String PAIMON_WRITE_OPTION_BATCH_INSERT = "batch_insert";
    public static final String PAIMON_WRITE_OPTION_STREAM_INSERT = "stream_insert";

    public static final String PAIMON_CATALOG_TYPE = "catalogType";
    /**
     * warehouse path
     */
    public static final String PAIMON_CATALOG_PATH = "catalogPath";
    public static final String PAIMON_TABLE_BUCKET = "tableBucket";
    public static final String PAIMON_METASTORE_TYPE = "metastoreType";
    /**
     * thrift://<hive-metastore-host-name>:<port>
     */
    public static final String PAIMON_METASTORE_URI = "metastoreUri";

    public static final String PAIMON_CATALOG_FILE = "file";
    public static final String PAIMON_CATALOG_HIVE = "hive";

    public static final String PAIMON_HIVE_CONF_DIR = "hiveConfDir";
    public static final String PAIMON_HADOOP_CONF_DIR = "hadoopConfDir";

    // Kerberos
    public static final String HAVE_KERBEROS = "haveKerberos";
    public static final String KERBEROS_KEYTAB_FILE_PATH = "kerberosKeytabFilePath";
    public static final String KERBEROS_PRINCIPAL = "kerberosPrincipal";

    public static final String HADOOP_SECURITY_AUTHENTICATION_KEY = "hadoop.security.authentication";

    // hadoop config
    public static final String HADOOP_CONFIG = "hadoopConfig";

    //paimon config
    public static final String PAIMON_CONFIG = "paimonConfig";

    //S3
    public static final String S3A_SSL = "fs.s3a.connection.ssl.enabled";
    public static final String S3A_PATH_STYLE_ACCESS = "fs.s3a.path.style.access";
    public static final String S3A_USER_NAME = "fs.s3a.access.key";
    public static final String S3A_USER_PWD = "fs.s3a.secret.key";
    public static final String S3A_ENDPOINT = "fs.s3a.endpoint";
    public static final String S3A_IMPL = "fs.s3a.impl";

}
