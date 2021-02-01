package com.alibaba.datax.core.statistics.container.collector;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;

public class ProcessInnerCollector extends AbstractCollector {

  public ProcessInnerCollector(Long jobId) {
    super.setJobId(jobId);
  }

  /**
   * 收集所有taskGroup的信息给tgManager
   *
   * @return Communication
   */
  @Override
  public Communication collectFromTaskGroup() {
    return LocalTGCommunicationManager.getJobCommunication();
  }

}
