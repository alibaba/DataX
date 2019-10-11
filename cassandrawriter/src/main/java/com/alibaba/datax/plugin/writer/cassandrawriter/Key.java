package com.alibaba.datax.plugin.writer.cassandrawriter;

/**
 * Created by mazhenlin on 2019/8/19.
 */
public class Key {
  public final static String USERNAME = "username";
  public final static String PASSWORD = "password";

  public final static String HOST = "host";
  public final static String PORT = "port";
  public final static String USESSL = "useSSL";

  public final static String KEYSPACE = "keyspace";
  public final static String TABLE = "table";
  public final static String COLUMN = "column";
  public final static String WRITE_TIME = "writetime()";
  public final static String ASYNC_WRITE = "asyncWrite";
  public final static String CONSITANCY_LEVEL = "consistancyLevel";
  public final static String CONNECTIONS_PER_HOST = "connectionsPerHost";
  public final static String MAX_PENDING_CONNECTION = "maxPendingPerConnection";
  /**
   * 异步写入的批次大小，默认1（不异步写入）
   */
  public final static String BATCH_SIZE = "batchSize";

  /**
   * 每个列的名字
   */
  public static final String COLUMN_NAME = "name";
  /**
   * 列分隔符
   */
  public static final String COLUMN_SPLITTER = "format";
  public static final String ELEMENT_SPLITTER = "splitter";
  public static final String ENTRY_SPLITTER = "entrySplitter";
  public static final String KV_SPLITTER = "kvSplitter";
  public static final String ELEMENT_CONFIG = "element";
  public static final String TUPLE_CONNECTOR = "_";
  public static final String KEY_CONFIG = "key";
  public static final String VALUE_CONFIG = "value";

}
