package com.alibaba.datax.core.statistics.plugin.task.log;

import com.alibaba.datax.common.element.Record;
import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author listening
 * @description
 * @date 2018-10-25 下午5:26
 */
public class ErrorLog {
  private static final Logger LOG = LoggerFactory.getLogger(ErrorLog.class);

  public static void persistMainInfo(String type, Record dirtyRecord) {
    Map<String, Object> err = new HashMap<String, Object>();
    err.put("type", type);
    if (dirtyRecord.getColumnNumber() >= 2) {
      Object cityId = dirtyRecord.getColumn(0).getRawData();
      Object pk = dirtyRecord.getColumn(1).getRawData();

      err.put("id", pk);
      err.put("cityId", cityId);
    }
    LOG.error(JSON.toJSONString(err));
  }
}
