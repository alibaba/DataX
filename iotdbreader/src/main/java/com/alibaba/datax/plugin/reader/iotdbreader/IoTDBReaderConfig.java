package com.alibaba.datax.plugin.reader.iotdbreader;

public class IoTDBReaderConfig {

  public static String NAME = "name";
  public static String ADDRESSES = "addresses";
  public static String USERNAME = "username";
  public static String PASSWORD = "password";
  public final static String SESSION_POOL_MAX_SIZE = "sessionPoolMaxSize";
  public final static int DEFAULT_SESSION_POOL_MAX_SIZE = 3;

  public final static String DATABASES = "databases";
  public final static int MEASUREMENT_OFFSET = 1;
  public final static String SENSORS = "sensors";
  public final static String DATA_TYPES = "dataTypes";
}
