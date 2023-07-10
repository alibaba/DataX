package com.alibaba.datax.plugin.writer.iotdbwriter;

public class IoTDBWriterConfig {

  public final static String ADDRESSES = "addresses";
  public final static String SESSION_POOL_MAX_SIZE = "sessionPoolMaxSize";
  public final static int DEFAULT_SESSION_POOL_MAX_SIZE = 3;

  public final static String USERNAME = "username";
  public final static String PASSWORD = "password";

  public final static String DATABASES = "databases";
  public final static String SENSORS = "sensors";
  public final static String DATA_TYPES = "dataTypes";

  public final static String BATCH_SIZE = "batchSize";
  public final static int DEFAULT_BATCH_SIZE = 32768;
}
