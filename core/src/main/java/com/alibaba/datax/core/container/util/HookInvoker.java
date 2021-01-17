package com.alibaba.datax.core.container.util;

/**
 * Created by xiafei.qiuxf on 14/12/17.
 */

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.Hook;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.JarLoader;
import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 扫描给定目录的所有一级子目录，每个子目录当作一个Hook的目录。 对于每个子目录，必须符合ServiceLoader的标准目录格式，
 * 见http://docs.oracle.com/javase/6/docs/api/java/util/ServiceLoader.html。
 * 加载里头的jar，使用ServiceLoader机制调用。
 */
public class HookInvoker {

  private static final Logger LOG = LoggerFactory.getLogger(HookInvoker.class);
  private final Map<String, Number> msg;
  private final Configuration conf;

  private File baseDir;

  public HookInvoker(String baseDirName, Configuration conf, Map<String, Number> msg) {
    this.baseDir = new File(baseDirName);
    this.conf = conf;
    this.msg = msg;
  }

  /**
   * 总体调用的方法
   */
  public void invokeAll() {
    if (!baseDir.exists() || baseDir.isFile()) {
      LOG.info("No hook invoked, because base dir not exists or is a file: " + baseDir
          .getAbsolutePath());
      return;
    }
    // 获取baseDir 下是文件夹的 目录名
    String[] subDirs = baseDir.list((dir, name) -> new File(dir, name).isDirectory());
    if (subDirs == null) {
      throw DataXException.asDataXException(FrameworkErrorCode.HOOK_LOAD_ERROR, "获取HOOK子目录返回null");
    }

    for (String subDir : subDirs) {
      doInvoke(new File(baseDir, subDir).getAbsolutePath());
    }
  }

  /**
   * 子调用方法。主要是加在该路径下得 jar
   * 1 先将当前classLoader暂存
   * 2 根据入参路径得到一个jarLoader
   * 3
   * @param path String
   */
  private void doInvoke(String path) {
    ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      JarLoader jarLoader = new JarLoader(new String[]{path});
      Thread.currentThread().setContextClassLoader(jarLoader);
      Iterator<Hook> hookIt = ServiceLoader.load(Hook.class).iterator();
      if (!hookIt.hasNext()) {
        LOG.warn("No hook defined under path: " + path);
      } else {
        Hook hook = hookIt.next();
        LOG.info("Invoke hook [{}], path: {}", hook.getName(), path);
        hook.invoke(conf, msg);
      }
    } catch (Exception e) {
      LOG.error("Exception when invoke hook", e);
      throw DataXException.asDataXException(
          CommonErrorCode.HOOK_INTERNAL_ERROR, "Exception when invoke hook", e);
    } finally {
      Thread.currentThread().setContextClassLoader(oldClassLoader);
    }
  }

  public static void main(String[] args) {
    ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
    System.out.println(oldClassLoader);
  }
}
