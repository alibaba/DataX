package com.alibaba.datax.core.statistics.container.collector;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 状态收集器
 */
public abstract class AbstractCollector {

  /**
   * taskCommunicationMap用于保存Task注册到TaskGroupContainer，当Task注册到TaskGroupContainer的时候将
   * TaskId和新建的Communication对象保存进taskCommunicationMap即可。
   */
  private Map<Integer, Communication> taskCommunicationMap = new ConcurrentHashMap<>();

  private Long jobId;

  public Map<Integer, Communication> getTaskCommunicationMap() {
    return taskCommunicationMap;
  }

  public Long getJobId() {
    return jobId;
  }

  public void setJobId(Long jobId) {
    this.jobId = jobId;
  }

  /**
   * 将TaskGroupContainer注册到JobContainer <br>
   * StandAloneJobContainerCommunicator将taskGroup的cfgs传入该类，该方法将taskGroupID和 comm
   * 托管到LocalTGCommunicationManager
   *
   * @param taskGroupConfigurationList List<Configuration>
   */
  public void registerTGCommunication(List<Configuration> taskGroupConfigurationList) {
    for (Configuration config : taskGroupConfigurationList) {
      int taskGroupId = config.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
      LocalTGCommunicationManager.registerTaskGroupCommunication(taskGroupId, new Communication());
    }
  }

  /**
   * Task注册到TaskGroupContainer
   *
   * @param taskConfigurationList List<Configuration>
   */
  public void registerTaskCommunication(List<Configuration> taskConfigurationList) {
    for (Configuration taskConfig : taskConfigurationList) {
      int taskId = taskConfig.getInt(CoreConstant.TASK_ID);
      this.taskCommunicationMap.put(taskId, new Communication());
    }
  }

  /**
   * 收集所有task信息
   *
   * @return Communication
   */
  public Communication collectFromTask() {
    Communication communication = new Communication();
    communication.setState(State.SUCCEEDED);

    for (Communication taskCommunication : this.taskCommunicationMap.values()) {
      communication.mergeFrom(taskCommunication);
    }
    return communication;
  }

  /**
   * 从tg里收集comm信息
   *
   * @return communication
   */
  public abstract Communication collectFromTaskGroup();

  public Map<Integer, Communication> getTGCommunicationMap() {
    return LocalTGCommunicationManager.getTaskGroupCommunicationMap();
  }

  public Communication getTGCommunication(Integer taskGroupId) {
    return LocalTGCommunicationManager.getTaskGroupCommunication(taskGroupId);
  }

  public Communication getTaskCommunication(Integer taskId) {
    return this.taskCommunicationMap.get(taskId);
  }
}
