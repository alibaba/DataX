package com.alibaba.datax.core.taskgroup.runner;

import com.alibaba.datax.common.plugin.AbstractTaskPlugin;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.apache.commons.lang.Validate;

public abstract class AbstractRunner {
    private AbstractTaskPlugin plugin;

    private Configuration jobConf;

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
        Validate.notNull(runnerCommunication,
                "插件的Communication不能为空");
        this.runnerCommunication = runnerCommunication;
    }

    public Communication getRunnerCommunication() {
        return runnerCommunication;
    }

    public abstract void shutdown();
}
