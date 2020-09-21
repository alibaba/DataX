package com.q1.datax.plugin.writer.kudu11xwriter;

/**
 * @author daizihao
 * @create 2020-08-31 14:42
 **/
public class Constant {
    public static final String DEFAULT_ENCODING = "UTF-8";
//    public static final String DEFAULT_DATA_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static final String COMPRESSION = "DEFAULT_COMPRESSION";
    public static final String ENCODING = "AUTO_ENCODING";
    public static final Long ADMIN_TIMEOUTMS = 60000L;
    public static final Long SESSION_TIMEOUTMS = 60000L;


    public static final String INSERT_MODE = "upsert";
    public static final long DEFAULT_WRITE_BATCH_SIZE = 512L;
    public static final long DEFAULT_MUTATION_BUFFER_SPACE = 3072L;

}
