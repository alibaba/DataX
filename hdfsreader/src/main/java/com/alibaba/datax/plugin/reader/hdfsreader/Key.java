package com.alibaba.datax.plugin.reader.hdfsreader;

public final class Key {

    /**
     * 此处声明插件用到的需要插件使用者提供的配置项
     */
    public final static String PATH = "path";
    public final static String DEFAULT_FS = "defaultFS";
    public static final String FILETYPE = "fileType";
    public static final String HADOOP_CONFIG = "hadoopConfig";
    public static final String HAVE_KERBEROS = "haveKerberos";
    public static final String KERBEROS_KEYTAB_FILE_PATH = "kerberosKeytabFilePath";
    public static final String KERBEROS_PRINCIPAL = "kerberosPrincipal";
}
