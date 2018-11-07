package com.alibaba.datax.plugin.reader.hbase11xsqlreader;

import org.apache.hadoop.hbase.HConstants;

public final class Key {

    public final static String MOCK_JOBID_IDENTIFIER = "phoenixreader";
    public final static int MOCK_JOBID = 1;
    public final static String SPLIT_KEY = "phoenixsplit";

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

}
