package com.alibaba.datax.core;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import org.apache.commons.lang.Validate;

/**
 * "执行容器"的抽象类，持有该容器全局的配置 configuration
 */
public abstract class AbstractContainer {

  protected Configuration configuration;

  protected AbstractContainerCommunicator containerCommunicator;


  public Configuration getConfiguration() {
    return configuration;
  }

  public AbstractContainerCommunicator getContainerCommunicator() {
    return containerCommunicator;
  }

  public void setContainerCommunicator(AbstractContainerCommunicator containerCommunicator) {
    this.containerCommunicator = containerCommunicator;
  }

  /**
   * 构造函数，根据cfg生成一个 抽取容器
   *
   * @param configuration Configuration
   */
  public AbstractContainer(Configuration configuration) {
    Validate.notNull(configuration, "Configuration can not be null.");
    this.configuration = configuration;
  }


  /**
   * 该“执行容器”的入口
   */
  public abstract void start();

}
