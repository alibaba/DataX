package com.alibaba.datax.core.taskgroup;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by liqiang on 15/7/23.
 * 任务监控类
 */
public class TaskMonitor {

  private static final Logger LOG = LoggerFactory.getLogger(TaskMonitor.class);
  private static final TaskMonitor instance = new TaskMonitor();
  private static long EXPIRED_TIME = 172800 * 1000;

  private ConcurrentHashMap<Integer, TaskCommunication> tasks = new ConcurrentHashMap<>();

  private TaskMonitor() {
  }

  public static TaskMonitor getInstance() {
    return instance;
  }

  public void registerTask(Integer taskId, Communication communication) {
    //如果task已经finish，直接返回
    if (communication.isFinished()) {
      return;
    }
    tasks.putIfAbsent(taskId, new TaskCommunication(taskId, communication));
  }

  public void removeTask(Integer taskId) {
    tasks.remove(taskId);
  }

  public void report(Integer taskId, Communication communication) {
    //如果task已经finish，直接返回
    if (communication.isFinished()) {
      return;
    }
    if (!tasks.containsKey(taskId)) {
      LOG.warn("unexpected: taskId({}) missed.", taskId);
      tasks.putIfAbsent(taskId, new TaskCommunication(taskId, communication));
    } else {
      tasks.get(taskId).report(communication);
    }
  }

  public TaskCommunication getTaskCommunication(Integer taskId) {
    return tasks.get(taskId);
  }


  public static class TaskCommunication {
    private Integer taskId;
    //记录最后更新的communication
    private long lastAllReadRecords = -1;
    //只有第一次，或者统计变更时才会更新TS
    private long lastUpdateCommunicationTS;
    private long ttl;

    private TaskCommunication(Integer taskId, Communication communication) {
      this.taskId = taskId;
      lastAllReadRecords = CommunicationTool.getTotalReadRecords(communication);
      ttl = System.currentTimeMillis();
      lastUpdateCommunicationTS = ttl;
    }

    public void report(Communication communication) {

      ttl = System.currentTimeMillis();
      //采集的数量增长，则变更当前记录, 优先判断这个条件，因为目的是不卡住，而不是expired
      if (CommunicationTool.getTotalReadRecords(communication) > lastAllReadRecords) {
        lastAllReadRecords = CommunicationTool.getTotalReadRecords(communication);
        lastUpdateCommunicationTS = ttl;
      } else if (isExpired(lastUpdateCommunicationTS)) {
        communication.setState(State.FAILED);
        communication.setTimestamp(ttl);
        communication
            .setThrowable(DataXException.asDataXException(CommonErrorCode.TASK_HUNG_EXPIRED,
                String.format("task(%s) hung expired [allReadRecord(%s), elased(%s)]", taskId,
                    lastAllReadRecords, (ttl - lastUpdateCommunicationTS))));
      }


    }

    private boolean isExpired(long lastUpdateCommunicationTS) {
      return System.currentTimeMillis() - lastUpdateCommunicationTS > EXPIRED_TIME;
    }

    public Integer getTaskId() {
      return taskId;
    }

    public long getLastAllReadRecords() {
      return lastAllReadRecords;
    }

    public long getLastUpdateCommunicationTS() {
      return lastUpdateCommunicationTS;
    }

    public long getTtl() {
      return ttl;
    }
  }
}
