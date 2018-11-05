package com.alibaba.datax.core.job;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.context.JobContext;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author listening
 * @description
 * @date 2018-10-26 上午10:01
 */
public class TaskResultLog {
  private static final Logger LOG = LoggerFactory.getLogger(TaskResultLog.class);

  private static final String RESULT_LOG_DIR = StringUtils.join(new String[] {
      CoreConstant.DATAX_HOME, "log"}, File.separator);

  public static void persist(String log) {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    String cur = dateFormat.format(new Date());

    String path = StringUtils.join(new String[] {RESULT_LOG_DIR, cur, "result.txt"}, File.separator);
    File file = createFile(path);
    if (file.exists()) {
      String taskKey = JobContext.get("fileName").toString();
      String taskName = getMapValue(taskKey);
      OutputStream out = null;
      try {
        out = new FileOutputStream(file, true);
        out.write(taskName.getBytes());
        out.write(log.getBytes());
        out.flush();
      } catch (IOException e) {
        LOG.error("持久化日志失败: " + path, e);
      } finally {
        if (out != null) {
          try {
            out.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    }
  }

  public static File createFile(String path) {
    /*File dir = new File(path).getParentFile();
    if (!dir.exists()) {
      dir.mkdirs();
    }*/

    File file = new File(path);
    if (!file.exists()) {
      try {
        file.createNewFile();
      } catch (IOException e) {
        LOG.error("文件创建失败, 创建文件为: " + path, e);
        e.printStackTrace();
      }
    }
    return file;
  }

  private static String getMapValue(String key) {
    String DATAX_APP_PATH = StringUtils.join(new String[] {
        CoreConstant.DATAX_HOME, "conf", "app.json" }, File.separator);
    Configuration conf = Configuration.from(new File(DATAX_APP_PATH));
    String taskName = conf.getString("taskMap." + key);
    return taskName == null ? key : taskName;
  }
}
