package com.alibaba.datax.core.job.scheduler.processinner;

import static com.alibaba.datax.core.util.FrameworkErrorCode.KILLED_EXIT_VALUE;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.scheduler.AbstractScheduler;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.taskgroup.TaskGroupContainer;
import com.alibaba.datax.core.taskgroup.runner.TaskGroupContainerRunner;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public abstract class ProcessInnerScheduler extends AbstractScheduler {

  private ExecutorService taskGroupContainerExecutorService;

  public ProcessInnerScheduler(AbstractContainerCommunicator containerCommunicator) {
    super(containerCommunicator);
  }

  @Override
  public void startAllTaskGroup(List<Configuration> cfgs) {
    this.taskGroupContainerExecutorService = new ThreadPoolExecutor(cfgs.size(), cfgs.size(),
        0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    for (Configuration taskGroupCfg : cfgs) {
      TaskGroupContainerRunner taskGroupContainerRunner = newTaskGroupContainerRunner(taskGroupCfg);
      this.taskGroupContainerExecutorService.execute(taskGroupContainerRunner);
    }
    this.taskGroupContainerExecutorService.shutdown();
  }

  @Override
  public void dealFailedStat(AbstractContainerCommunicator frameworkCollector,
      Throwable throwable) {
    this.taskGroupContainerExecutorService.shutdownNow();
    throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_RUNTIME_ERROR, throwable);
  }


  @Override
  public void dealKillingStat(AbstractContainerCommunicator frameworkCollector, int totalTasks) {
    //通过进程退出返回码标示状态
    this.taskGroupContainerExecutorService.shutdownNow();
    throw DataXException.asDataXException(KILLED_EXIT_VALUE, "job killed status");
  }


  private TaskGroupContainerRunner newTaskGroupContainerRunner(Configuration configuration) {
    TaskGroupContainer taskGroupContainer = new TaskGroupContainer(configuration);
    return new TaskGroupContainerRunner(taskGroupContainer);
  }

}
