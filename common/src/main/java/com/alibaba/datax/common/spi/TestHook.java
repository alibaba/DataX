package com.alibaba.datax.common.spi;

import com.alibaba.datax.common.util.Configuration;
import java.util.Map;

public class TestHook implements Hook {

  @Override
  public String getName() {
    return null;
  }

  @Override
  public void invoke(Configuration jobConf, Map<String, Number> msg) {
    System.out.println(jobConf.toJSON());
  }
}
