package com.alibaba.datax.plugin.writer.hbase11xsqlwriter;

import org.apache.hadoop.hbase.HConstants;

public final class Key {

    /**
     * 【必选】hbase集群配置，连接一个hbase集群需要的最小配置只有两个：zk和znode
     */
    public final static String HBASE_CONFIG = "hbaseConfig";
    public final static String HBASE_ZK_QUORUM = HConstants.ZOOKEEPER_QUORUM;
    public final static String HBASE_ZNODE_PARENT = HConstants.ZOOKEEPER_ZNODE_PARENT;

    /**
     * 【必选】writer要写入的表的表名
     */
    public final static String TABLE = "table";

    /**
     * 【必选】列配置
     */
    public final static String COLUMN = "column";
    public static final String NAME = "name";

    /**
     * 【可选】遇到空值默认跳过
     */
    public static  final String NULL_MODE = "nullMode";

    /**
     * 【可选】
     * 在writer初始化的时候，是否清空目的表
     * 如果全局启动多个writer，则必须确保所有的writer都prepare之后，再开始导数据。
     */
    public static  final String TRUNCATE = "truncate";

    /**
     * 【可选】批量写入的最大行数，默认100行
     */
    public static  final String BATCH_SIZE = "batchSize";



}
