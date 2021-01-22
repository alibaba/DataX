package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.core.statistics.communication.Communication;

/**
 * Reporter的主要功能是将收集到的信息上报给上级
 */
public abstract class AbstractReporter {

  /**
   * 汇报job的communication信息给上级（目前datax中job无上级，所以子类中do nothing）
   *
   * @param jobId         Long
   * @param communication Communication
   */
  public abstract void reportJobCommunication(Long jobId, Communication communication);

  /**
   * 汇报TG的communication信息给上级
   *
   * @param taskGroupId   Integer
   * @param communication Communication
   */
  public abstract void reportTGCommunication(Integer taskGroupId, Communication communication);

}
