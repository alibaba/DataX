package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;

public class ProcessInnerReporter extends AbstractReporter {

  /**
   * 汇报job的communication信息给上级（目前datax中job无上级，所以do nothing）
   *
   * @param jobId         Integer
   * @param communication Communication
   */
  @Override
  public void reportJobCommunication(Long jobId, Communication communication) {
    // do nothing
  }

  /**
   * 将TaskGroupContainer的信息汇报给上级，操作也很简单直接更新注册时分配给该TaskGroup的Communication（Map中的值）
   *
   * @param taskGroupId   Integer
   * @param communication Communication
   */
  @Override
  public void reportTGCommunication(Integer taskGroupId, Communication communication) {
    LocalTGCommunicationManager.updateTaskGroupCommunication(taskGroupId, communication);
  }
}