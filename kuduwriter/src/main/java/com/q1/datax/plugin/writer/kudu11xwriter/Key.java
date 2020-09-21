package com.q1.datax.plugin.writer.kudu11xwriter;

/**
 * @author daizihao
 * @create 2020-08-31 14:17
 **/
public class Key {
    public final static String KUDU_CONFIG = "kuduConfig";
    public final static String KUDU_MASTER = "kudu.master_addresses";
    public final static String KUDU_ADMIN_TIMEOUT = "timeout";
    public final static String KUDU_SESSION_TIMEOUT = "sessionTimeout";

    public final static String TABLE = "table";
    public final static String PARTITION = "partition";
    public final static String COLUMN = "column";

    public static final String NAME = "name";
    public static final String TYPE = "type";
    public static final String INDEX = "index";
    public static final String PRIMARYKEY = "primaryKey";
    public static final String COMPRESSION = "compress";
    public static final String COMMENT = "comment";
    public final static String ENCODING = "encoding";



    public static final String NUM_REPLICAS = "replicaCount";
    public static final String HASH = "hash";
    public static final String HASH_NUM = "number";

    public static final String RANGE = "range";
    public static final String LOWER = "lower";
    public static final String UPPER = "upper";



    public static  final String TRUNCATE = "truncate";

    public static final String INSERT_MODE = "writeMode";

    public static  final String WRITE_BATCH_SIZE = "batchSize";

    public static  final String MUTATION_BUFFER_SPACE = "bufferSize";
    public static  final String SKIP_FAIL = "skipFail";
}
