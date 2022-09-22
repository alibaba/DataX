package com.alibaba.datax.plugin.writer.datahubwriter;

public final class Key {

    /**
     * 此处声明插件用到的需要插件使用者提供的配置项
     */
    public static final String CONFIG_KEY_ENDPOINT = "endpoint";
    public static final String CONFIG_KEY_ACCESS_ID = "accessId";
    public static final String CONFIG_KEY_ACCESS_KEY = "accessKey";
    public static final String CONFIG_KEY_PROJECT = "project";
    public static final String CONFIG_KEY_TOPIC = "topic";
    public static final String CONFIG_KEY_WRITE_MODE = "mode";
    public static final String CONFIG_KEY_SHARD_ID = "shardId";
    public static final String CONFIG_KEY_MAX_COMMIT_SIZE = "maxCommitSize";
    public static final String CONFIG_KEY_MAX_RETRY_COUNT = "maxRetryCount";

    public static final String CONFIG_VALUE_SEQUENCE_MODE = "sequence";
    public static final String CONFIG_VALUE_RANDOM_MODE = "random";
    
    public final static String MAX_RETRY_TIME = "maxRetryTime";
    
    public final static String RETRY_INTERVAL = "retryInterval";
    
    public final static String CONFIG_KEY_COLUMN = "column";
}
