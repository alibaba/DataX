package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.core.statistics.communication.Communication;

/**
 * Reporter的主要功能是将收集到的信息上报给上级
 */
public abstract class AbstractReporter {

  public abstract void reportJobCommunication(Long jobId, Communication communication);

  public abstract void reportTGCommunication(Integer taskGroupId, Communication communication);

}
