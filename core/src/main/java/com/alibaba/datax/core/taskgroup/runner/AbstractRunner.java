package com.alibaba.datax.core.taskgroup.runner;

import com.alibaba.datax.common.plugin.AbstractTaskPlugin;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.apache.commons.lang.Validate;

public abstract class AbstractRunner {

  /**
   * 基类任务插件
   */
  private AbstractTaskPlugin plugin;

  /**
   * 任务的配置信息
   */
  private Configuration jobConf;

  /**
   * runner的通讯类，里面可以记录该runner的信息
   */
  private Communication runnerCommunication;

  private int taskGroupId;

  private int taskId;

  public AbstractRunner(AbstractTaskPlugin taskPlugin) {
    this.plugin = taskPlugin;
  }

  public void destroy() {
    if (this.plugin != null) {
      this.plugin.destroy();
    }
  }

  public State getRunnerState() {
    return this.runnerCommunication.getState();
  }

  public AbstractTaskPlugin getPlugin() {
    return plugin;
  }

  public void setPlugin(AbstractTaskPlugin plugin) {
    this.plugin = plugin;
  }

  public Configuration getJobConf() {
    return jobConf;
  }

  public void setJobConf(Configuration jobConf) {
    this.jobConf = jobConf;
    this.plugin.setPluginJobConf(jobConf);
  }

  public void setTaskPluginCollector(TaskPluginCollector pluginCollector) {
    this.plugin.setTaskPluginCollector(pluginCollector);
  }

  /**
   * 给正在运行的communication状态加1，实现标记
   *
   * @param state State
   */
  private void mark(State state) {
    this.runnerCommunication.setState(state);
    if (state == State.SUCCEEDED) {
      // 对 stage + 1
      this.runnerCommunication.setLongCounter(CommunicationTool.STAGE,
          this.runnerCommunication.getLongCounter(CommunicationTool.STAGE) + 1);
    }
  }

  public void markRun() {
    mark(State.RUNNING);
  }

  public void markSuccess() {
    mark(State.SUCCEEDED);
  }

  public void markFail(final Throwable throwable) {
    mark(State.FAILED);
    this.runnerCommunication.setTimestamp(System.currentTimeMillis());
    this.runnerCommunication.setThrowable(throwable);
  }

  /**
   * @param taskGroupId the taskGroupId to set
   */
  public void setTaskGroupId(int taskGroupId) {
    this.taskGroupId = taskGroupId;
    this.plugin.setTaskGroupId(taskGroupId);
  }

  /**
   * @return the taskGroupId
   */
  public int getTaskGroupId() {
    return taskGroupId;
  }

  public int getTaskId() {
    return taskId;
  }

  public void setTaskId(int taskId) {
    this.taskId = taskId;
    this.plugin.setTaskId(taskId);
  }

  public void setRunnerCommunication(final Communication runnerCommunication) {
    Validate.notNull(runnerCommunication, "插件的Communication不能为空");
    this.runnerCommunication = runnerCommunication;
  }

  public Communication getRunnerCommunication() {
    return runnerCommunication;
  }

  /**
   * 任务关闭
   */
  public abstract void shutdown();
}
