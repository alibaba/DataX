package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;

public class ProcessInnerReporter extends AbstractReporter {

  @Override
  public void reportJobCommunication(Long jobId, Communication communication) {
    // do nothing
  }

  /**
   * 将TaskGroupContianer的信息汇报给上级，操作也很简单直接更新注册时分配给该TaskGroup的Communication（Map中的值）
   *
   * @param taskGroupId   Integer
   * @param communication Communication
   */
  @Override
  public void reportTGCommunication(Integer taskGroupId, Communication communication) {
    LocalTGCommunicationManager.updateTaskGroupCommunication(taskGroupId, communication);
  }
}