package com.alibaba.datax.core.context;

import java.util.HashMap;
import java.util.Map;

/**
 * @author listening
 * @description
 * @date 2018-10-26 下午3:42
 */
public class JobContext {
  private static final ThreadLocal<Map<Object, Object>> currentThread = new ThreadLocal<Map<Object, Object>>();

  public static synchronized <K, V> void set(K key, V value) {
    Map data = currentThread.get();
    if (data == null) {
      Map<Object,Object> map = new HashMap<Object, Object>();
      map.put(key,value);
      currentThread.set(map);
    } else {
      data.put(key, value);
    }
  }

  public static <K, V> V get(K key) {
    Map data = currentThread.get();
    if (data != null) {
      return (V) data.get(key);
    }
    return null;
  }

  public static Map get() {
    return currentThread.get();
  }
}
