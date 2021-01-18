package com.alibaba.datax.core.statistics.container.communicator.taskgroup;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.container.report.ProcessInnerReporter;
import com.alibaba.datax.core.statistics.communication.Communication;

/**
 * 独立模式的taskGroup 的通讯类
 */
public class StandaloneTGContainerCommunicator extends AbstractTGContainerCommunicator {

  /**
   * 单机版的容器沟通者(独立模式的taskGroup 的通讯类)
   *
   * @param configuration
   */
  public StandaloneTGContainerCommunicator(Configuration configuration) {
    super(configuration);
    super.setReporter(new ProcessInnerReporter());
  }

  @Override
  public void report(Communication communication) {
    super.getReporter().reportTGCommunication(super.taskGroupId, communication);
  }

}
