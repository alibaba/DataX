package com.alibaba.datax.core.taskgroup;

import static com.alibaba.datax.common.exception.CommonErrorCode.WAIT_TIME_EXCEED;
import static com.alibaba.datax.core.util.container.CoreConstant.DATAX_CORE_CONTAINER_JOB_ID;
import static com.alibaba.datax.core.util.container.CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL;
import static com.alibaba.datax.core.util.container.CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID;
import static com.alibaba.datax.core.util.container.CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_REPORTINTERVAL;
import static com.alibaba.datax.core.util.container.CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_SLEEPINTERVAL;
import static com.alibaba.datax.core.util.container.CoreConstant.DATAX_CORE_CONTAINER_TASK_FAILOVER_MAXRETRYTIMES;
import static com.alibaba.datax.core.util.container.CoreConstant.DATAX_CORE_CONTAINER_TASK_FAILOVER_MAXWAITINMSEC;
import static com.alibaba.datax.core.util.container.CoreConstant.DATAX_CORE_CONTAINER_TASK_FAILOVER_RETRYINTERVALINMSEC;
import static com.alibaba.datax.core.util.container.CoreConstant.DATAX_CORE_STATISTICS_COLLECTOR_PLUGIN_TASKCLASS;
import static com.alibaba.datax.core.util.container.CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CLASS;
import static com.alibaba.datax.core.util.container.CoreConstant.DATAX_JOB_CONTENT;
import static com.alibaba.datax.core.util.container.CoreConstant.JOB_READER_NAME;
import static com.alibaba.datax.core.util.container.CoreConstant.JOB_READER_PARAMETER;
import static com.alibaba.datax.core.util.container.CoreConstant.JOB_WRITER_NAME;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.AbstractContainer;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.container.communicator.taskgroup.StandaloneTGContainerCommunicator;
import com.alibaba.datax.core.statistics.plugin.task.AbstractTaskPluginCollector;
import com.alibaba.datax.core.taskgroup.runner.AbstractRunner;
import com.alibaba.datax.core.taskgroup.runner.ReaderRunner;
import com.alibaba.datax.core.taskgroup.runner.WriterRunner;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.transport.exchanger.BufferedRecordExchanger;
import com.alibaba.datax.core.transport.exchanger.BufferedRecordTransformerExchanger;
import com.alibaba.datax.core.transport.transformer.TransformerExecution;
import com.alibaba.datax.core.util.ClassUtil;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.TransformerUtil;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import com.alibaba.fastjson.JSON;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JobContainer将所有的task分配到TaskGroup中执行，TaskGroup启动5个线程去消费所有的task
 */
public class TaskGroupContainer extends AbstractContainer {

  private static final Logger LOG = LoggerFactory.getLogger(TaskGroupContainer.class);

  /**
   * 当前taskGroup所属jobId
   */
  private long jobId;

  /**
   * 当前taskGroupId
   */
  private int taskGroupId;

  /**
   * 使用的channel类
   */
  private String channelClz;

  /**
   * task收集器使用的类
   */
  private String taskCollectClz;

  private TaskMonitor taskMonitor = TaskMonitor.getInstance();


  public TaskGroupContainer(Configuration cfg) {
    super(cfg);
    initCommunicator(cfg);

    this.jobId = configuration.getLong(DATAX_CORE_CONTAINER_JOB_ID);
    this.taskGroupId = configuration.getInt(DATAX_CORE_CONTAINER_TASKGROUP_ID);
    this.channelClz = configuration.getString(DATAX_CORE_TRANSPORT_CHANNEL_CLASS);
    this.taskCollectClz = configuration.getString(DATAX_CORE_STATISTICS_COLLECTOR_PLUGIN_TASKCLASS);
  }

  /**
   * 初始化容器之间的沟通者
   *
   * @param configuration
   */
  private void initCommunicator(Configuration configuration) {
    super.setContainerCommunicator(new StandaloneTGContainerCommunicator(configuration));

  }

  public long getJobId() {
    return jobId;
  }

  public int getTaskGroupId() {
    return taskGroupId;
  }

  /**
   * 1、初始化task执行相关的状态信息，分别是taskId->Cfg的map、待运行的任务队列taskQueue、运行失败任务taskFailedExecutorMap、
   * 运行中的任务runTasks、任务开始时间taskStartTimeMap
   * 2、循环检测所有任务的执行状态
   * 1）判断是否有失败的task，如果有则放入失败对立中，并查看当前的执行是否支持重跑和failOver，如果支持则重新放回执行队列中；
   * 如果没有失败，则标记任务执行成功，并从状态轮询map中移除
   * 2）如果发现有失败的任务，则汇报当前TaskGroup的状态，并抛出异常
   * 3）查看当前执行队列的长度，如果发现执行队列还有通道，则构建TaskExecutor加入执行队列，并从待运行移除
   * 4）检查执行队列和所有的任务状态，如果所有的任务都执行成功，则汇报taskGroup的状态并从循环中退出
   * 5）检查当前时间是否超过汇报时间检测，如果是，则汇报当前状态
   * 6）当所有的执行完成从while中退出之后，再次全局汇报当前的任务状态
   */
  @Override
  public void start() {
    try {
      //状态check时间间隔，较短，可以把任务及时分发到对应channel中
      int sleepMsec = configuration.getInt(DATAX_CORE_CONTAINER_TASKGROUP_SLEEPINTERVAL, 100);
      //状态汇报时间间隔，稍长，避免大量汇报
      long reportMsec = configuration.getLong(DATAX_CORE_CONTAINER_TASKGROUP_REPORTINTERVAL, 10000);
      /**
       * 2分钟汇报一次性能统计
       */
      // 获取channel数目
      int channelNum = configuration.getInt(DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL);
      int taskMaxRetrys = configuration.getInt(DATAX_CORE_CONTAINER_TASK_FAILOVER_MAXRETRYTIMES, 1);
      long taskRetryMsec = configuration
          .getLong(DATAX_CORE_CONTAINER_TASK_FAILOVER_RETRYINTERVALINMSEC, 10000);
      long taskMaxWaitInMsec = configuration
          .getLong(DATAX_CORE_CONTAINER_TASK_FAILOVER_MAXWAITINMSEC, 60000);

      List<Configuration> taskCfgs = configuration.getListConfiguration(DATAX_JOB_CONTENT);

      if (LOG.isDebugEnabled()) {
        LOG.debug("taskGroup[{}]'s task configs[{}]", taskGroupId, JSON.toJSONString(taskCfgs));
      }

      int taskCntInThisTaskGroup = taskCfgs.size();
      LOG.info(String.format("taskGroupId=[%d] start [%d] channels for [%d] tasks.",
          this.taskGroupId, channelNum, taskCntInThisTaskGroup));

      this.containerCommunicator.registerCommunication(taskCfgs);
      //taskId与task配置
      Map<Integer, Configuration> taskConfigMap = buildTaskConfigMap(taskCfgs);
      //待运行task列表
      List<Configuration> taskQueue = buildRemainTasks(taskCfgs);
      //taskId与上次失败实例
      Map<Integer, TaskExecutor> taskFailedExecutorMap = new HashMap<>();
      //正在运行task
      List<TaskExecutor> runTasks = new ArrayList<>(channelNum);
      //任务开始时间
      Map<Integer, Long> taskStartTimeMap = new HashMap<>();

      long lastReportTimeStamp = 0;
      Communication lastTaskGroupContainerComm = new Communication();

      while (true) {
        //1.判断task状态
        boolean failedOrKilled = false;
        //因为实现是TGContainerCommunicator，所以返回是 Map: key=taskId, value=Communication
        Map<Integer, Communication> communicationMap = containerCommunicator.getCommunicationMap();
        for (Map.Entry<Integer, Communication> entry : communicationMap.entrySet()) {
          Integer taskId = entry.getKey();
          Communication taskCommunication = entry.getValue();
          // 通讯类没有结束，就继续执行后面代码
          if (!taskCommunication.isFinished()) {
            continue;
          }
          // 任务正在执行，所以从runTasks中根据taskId移除
          TaskExecutor taskExecutor = removeTask(runTasks, taskId);
          //上面从runTasks里移除了，因此对应在monitor里移除
          taskMonitor.removeTask(taskId);

          //失败，看task是否支持failover，重试次数未超过最大限制
          if (taskCommunication.getState() == State.FAILED) {
            taskFailedExecutorMap.put(taskId, taskExecutor);
            // 如果 任务支持失败重试，并且重试次数小于 任务最大重试次数，则重新将任务加入到队列
            if (taskExecutor.supportFailOver() && taskExecutor.getAttemptCount() < taskMaxRetrys) {
              //关闭老的executor
              taskExecutor.shutdown();
              //将task的状态重置
              containerCommunicator.resetCommunication(taskId);
              Configuration taskConfig = taskConfigMap.get(taskId);
              //重新加入任务列表
              taskQueue.add(taskConfig);
            } else {
              failedOrKilled = true;
              break;
            }
          } else if (taskCommunication.getState() == State.KILLED) {
            failedOrKilled = true;
            break;
          } else if (taskCommunication.getState() == State.SUCCEEDED) {
            // 如果 task成功，将该信息记录到性能记录类PerfRecord（方便统计耗时最长的task）
            Long start = taskStartTimeMap.get(taskId);
            if (start != null) {
              Long cost = System.currentTimeMillis() - start;
              LOG.info("taskGroup[{}] taskId[{}] is succeed,used[{}]ms", taskGroupId, taskId, cost);
              //cost*1000*1000 转换成PerfRecord记录的ns，这里主要是简单登记，进行最长任务的打印。因此增加特定静态方法
              long ns = cost * 1000 * 1000L;
              PerfRecord.addPerfRecord(taskGroupId, taskId, PerfRecord.PHASE.TASK_TOTAL, start, ns);
              taskStartTimeMap.remove(taskId);
              taskConfigMap.remove(taskId);
            }
          }
        }

        // 2.发现该taskGroup下taskExecutor的总状态失败则汇报错误
        if (failedOrKilled) {
          lastTaskGroupContainerComm = reportTaskGroupCommunication(lastTaskGroupContainerComm,
              taskCntInThisTaskGroup);

          throw DataXException.asDataXException(
              FrameworkErrorCode.PLUGIN_RUNTIME_ERROR, lastTaskGroupContainerComm.getThrowable());
        }

        //3.有任务未执行，且正在运行的任务数小于最大通道限制
        Iterator<Configuration> iterator = taskQueue.iterator();
        while (iterator.hasNext() && runTasks.size() < channelNum) {
          Configuration taskConfig = iterator.next();
          Integer taskId = taskConfig.getInt(CoreConstant.TASK_ID);
          int attemptCount = 1;
          TaskExecutor lastExecutor = taskFailedExecutorMap.get(taskId);
          if (lastExecutor != null) {
            attemptCount = lastExecutor.getAttemptCount() + 1;
            long now = System.currentTimeMillis();
            long failedTime = lastExecutor.getTimeStamp();
            //未到等待时间，继续留在队列
            if (now - failedTime < taskRetryMsec) {
              continue;
            }
            //上次失败的task仍未结束
            if (!lastExecutor.isShutdown()) {
              if (now - failedTime > taskMaxWaitInMsec) {
                markCommunicationFailed(taskId);
                reportTaskGroupCommunication(lastTaskGroupContainerComm, taskCntInThisTaskGroup);
                throw DataXException.asDataXException(WAIT_TIME_EXCEED, "task failover等待超时");
              } else {
                lastExecutor.shutdown(); //再次尝试关闭
                continue;
              }
            } else {
              LOG.info("taskGroup[{}] taskId[{}] attemptCount[{}] has already shutdown",
                  this.taskGroupId, taskId, lastExecutor.getAttemptCount());
            }
          }
          Configuration taskConfigForRun = taskMaxRetrys > 1 ? taskConfig.clone() : taskConfig;
          TaskExecutor taskExecutor = new TaskExecutor(taskConfigForRun, attemptCount);
          taskStartTimeMap.put(taskId, System.currentTimeMillis());
          taskExecutor.doStart();

          iterator.remove();
          runTasks.add(taskExecutor);

          //上面，增加task到runTasks列表，因此在monitor里注册。
          taskMonitor.registerTask(taskId, this.containerCommunicator.getCommunication(taskId));

          taskFailedExecutorMap.remove(taskId);
          LOG.info("taskGroup[{}] taskId[{}] attemptCount[{}] is started",
              this.taskGroupId, taskId, attemptCount);
        }

        //4.任务列表为空，executor已结束, 搜集状态为success--->成功
        if (taskQueue.isEmpty() && isAllTaskDone(runTasks)
            && containerCommunicator.collectState() == State.SUCCEEDED) {
          // 成功的情况下，也需要汇报一次。否则在任务结束非常快的情况下，采集的信息将会不准确
          lastTaskGroupContainerComm = reportTaskGroupCommunication(lastTaskGroupContainerComm,
              taskCntInThisTaskGroup);

          LOG.info("taskGroup[{}] completed it's tasks.", this.taskGroupId);
          break;
        }

        // 5.如果当前时间已经超出汇报时间的interval，那么我们需要马上汇报
        long now = System.currentTimeMillis();
        if (now - lastReportTimeStamp > reportMsec) {
          lastTaskGroupContainerComm = reportTaskGroupCommunication(lastTaskGroupContainerComm,
              taskCntInThisTaskGroup);

          lastReportTimeStamp = now;

          //taskMonitor对于正在运行的task，每reportIntervalInMillSec进行检查
          for (TaskExecutor taskExecutor : runTasks) {
            taskMonitor.report(taskExecutor.getTaskId(),
                this.containerCommunicator.getCommunication(taskExecutor.getTaskId()));
          }
        }
        Thread.sleep(sleepMsec);
      }

      //6.最后还要汇报一次
      reportTaskGroupCommunication(lastTaskGroupContainerComm, taskCntInThisTaskGroup);

    } catch (Throwable e) {
      Communication nowTaskGroupContainerCommunication = this.containerCommunicator.collect();
      if (nowTaskGroupContainerCommunication.getThrowable() == null) {
        nowTaskGroupContainerCommunication.setThrowable(e);
      }
      nowTaskGroupContainerCommunication.setState(State.FAILED);
      this.containerCommunicator.report(nowTaskGroupContainerCommunication);

      throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, e);
    } finally {
      if (!PerfTrace.getInstance().isJob()) {
        //最后打印cpu的平均消耗，GC的统计
        VMInfo vmInfo = VMInfo.getVmInfo();
        if (vmInfo != null) {
          vmInfo.getDelta(false);
          LOG.info(vmInfo.totalString());
        }
        LOG.info(PerfTrace.getInstance().summarizeNoException());
      }
    }
  }

  private Map<Integer, Configuration> buildTaskConfigMap(List<Configuration> configurations) {
    Map<Integer, Configuration> map = new HashMap<>();
    for (Configuration taskConfig : configurations) {
      int taskId = taskConfig.getInt(CoreConstant.TASK_ID);
      map.put(taskId, taskConfig);
    }
    return map;
  }

  /**
   * 构建剩余未运行的task。形成一个queue
   *
   * @param configurations
   * @return List<Configuration> LinkedList 类型，可以保证任务的有序
   */
  private List<Configuration> buildRemainTasks(List<Configuration> configurations) {
    List<Configuration> remainTasks = new LinkedList<>();
    for (Configuration taskConfig : configurations) {
      remainTasks.add(taskConfig);
    }
    return remainTasks;
  }

  private TaskExecutor removeTask(List<TaskExecutor> taskList, int taskId) {
    Iterator<TaskExecutor> iterator = taskList.iterator();
    while (iterator.hasNext()) {
      TaskExecutor taskExecutor = iterator.next();
      if (taskExecutor.getTaskId() == taskId) {
        iterator.remove();
        return taskExecutor;
      }
    }
    return null;
  }

  private boolean isAllTaskDone(List<TaskExecutor> taskList) {
    for (TaskExecutor taskExecutor : taskList) {
      if (!taskExecutor.isTaskFinished()) {
        return false;
      }
    }
    return true;
  }

  /**
   * TaskGroupContainer向JobContainer汇报
   *
   * @param lastTGContainerComm
   * @param taskCnt
   * @return
   */
  private Communication reportTaskGroupCommunication(Communication lastTGContainerComm,int taskCnt) {
    Communication nowTGContainerComm = this.containerCommunicator.collect();
    nowTGContainerComm.setTimestamp(System.currentTimeMillis());
    Communication reportComm = CommunicationTool
        .getReportCommunication(nowTGContainerComm, lastTGContainerComm, taskCnt);
    this.containerCommunicator.report(reportComm);
    return reportComm;
  }

  private void markCommunicationFailed(Integer taskId) {
    Communication communication = containerCommunicator.getCommunication(taskId);
    communication.setState(State.FAILED);
  }

  /**
   * TaskExecutor是一个完整task的执行器
   * 其中包括1：1的reader和writer
   */
  class TaskExecutor {

    private Configuration taskConfig;

    private int taskId;

    private int attemptCount;

    private Channel channel;

    private Thread readerThread;

    private Thread writerThread;

    private ReaderRunner readerRunner;

    private WriterRunner writerRunner;

    /**
     * 该处的taskCommunication在多处用到：
     * 1. channel
     * 2. readerRunner和writerRunner
     * 3. reader和writer的taskPluginCollector
     */
    private Communication taskCommunication;

    public TaskExecutor(Configuration taskConf, int attemptCount) {
      // 获取该taskExecutor的配置
      this.taskConfig = taskConf;
      Validate.isTrue(null != this.taskConfig.getConfiguration(CoreConstant.JOB_READER)
              && null != this.taskConfig.getConfiguration(CoreConstant.JOB_WRITER),
          "[reader|writer]的插件参数不能为空!");

      // 得到taskId
      this.taskId = this.taskConfig.getInt(CoreConstant.TASK_ID);
      this.attemptCount = attemptCount;

      /**
       * 由taskId得到该taskExecutor的Communication
       * 要传给readerRunner和writerRunner，同时要传给channel作统计用
       */
      this.taskCommunication = containerCommunicator
          .getCommunication(taskId);
      Validate.notNull(this.taskCommunication,
          String.format("taskId[%d]的Communication没有注册过", taskId));
      this.channel = ClassUtil.instantiate(channelClz,
          Channel.class, configuration);
      this.channel.setCommunication(this.taskCommunication);

      /**
       * 获取transformer的参数
       */

      List<TransformerExecution> transformerInfoExecs = TransformerUtil
          .buildTransformerInfo(taskConfig);

      /**
       * 生成writerThread
       */
      writerRunner = (WriterRunner) generateRunner(PluginType.WRITER);
      this.writerThread = new Thread(writerRunner,
          String.format("%d-%d-%d-writer", jobId, taskGroupId, this.taskId));
      //通过设置thread的contextClassLoader，即可实现同步和主程序不通的加载器
      this.writerThread.setContextClassLoader(LoadUtil.getJarLoader(
          PluginType.WRITER, this.taskConfig.getString(JOB_WRITER_NAME)));

      /**
       * 生成readerThread
       */
      readerRunner = (ReaderRunner) generateRunner(PluginType.READER, transformerInfoExecs);
      this.readerThread = new Thread(readerRunner,
          String.format("%d-%d-%d-reader", jobId, taskGroupId, this.taskId));
      /**
       * 通过设置thread的contextClassLoader，即可实现同步和主程序不通的加载器
       */
      this.readerThread.setContextClassLoader(LoadUtil.getJarLoader(
          PluginType.READER, this.taskConfig.getString(JOB_READER_NAME)));
    }

    /**
     * 具体的start
     */
    public void doStart() {
      this.writerThread.start();
      // reader没有起来，writer不可能结束
      if (!this.writerThread.isAlive() || this.taskCommunication.getState() == State.FAILED) {
        throw DataXException.asDataXException(
            FrameworkErrorCode.RUNTIME_ERROR, this.taskCommunication.getThrowable());
      }

      this.readerThread.start();
      // 这里reader可能很快结束
      if (!this.readerThread.isAlive() && this.taskCommunication.getState() == State.FAILED) {
        // 这里有可能出现Reader线上启动即挂情况 对于这类情况 需要立刻抛出异常
        throw DataXException.asDataXException(
            FrameworkErrorCode.RUNTIME_ERROR, this.taskCommunication.getThrowable());
      }
    }


    private AbstractRunner generateRunner(PluginType pluginType) {
      return generateRunner(pluginType, null);
    }

    /**
     * 根据插件类型+ 转换执行器，生成抽象的运行器（readerRunner或 writerRunner）
     *
     * @param pluginType       PluginType
     * @param transformerExecs List<TransformerExecution>
     * @return AbstractRunner
     */
    private AbstractRunner generateRunner(PluginType pluginType,
        List<TransformerExecution> transformerExecs) {

      AbstractRunner newRunner;
      TaskPluginCollector pluginCollector;

      switch (pluginType) {
        case READER:
          newRunner = LoadUtil.loadPluginRunner(pluginType, taskConfig.getString(JOB_READER_NAME));
          newRunner.setJobConf(this.taskConfig.getConfiguration(JOB_READER_PARAMETER));

          pluginCollector = ClassUtil.instantiate(taskCollectClz, AbstractTaskPluginCollector.class,
              configuration, this.taskCommunication, PluginType.READER);

          RecordSender recordSender;
          if (transformerExecs != null && transformerExecs.size() > 0) {
            recordSender = new BufferedRecordTransformerExchanger(taskGroupId, this.taskId,
                this.channel, this.taskCommunication, pluginCollector, transformerExecs);
          } else {
            recordSender = new BufferedRecordExchanger(this.channel, pluginCollector);
          }
          ((ReaderRunner) newRunner).setRecordSender(recordSender);

          /**
           * 设置taskPlugin的collector，用来处理脏数据和job/task通信
           */
          newRunner.setTaskPluginCollector(pluginCollector);
          break;
        case WRITER:
          newRunner = LoadUtil.loadPluginRunner(pluginType, taskConfig.getString(JOB_WRITER_NAME));
          newRunner.setJobConf(this.taskConfig.getConfiguration(CoreConstant.JOB_WRITER_PARAMETER));

          pluginCollector = ClassUtil.instantiate(taskCollectClz, AbstractTaskPluginCollector.class,
              configuration, this.taskCommunication, PluginType.WRITER);
          ((WriterRunner) newRunner).setRecordReceiver(new BufferedRecordExchanger(
              this.channel, pluginCollector));
          /**
           * 设置taskPlugin的collector，用来处理脏数据和job/task通信
           */
          newRunner.setTaskPluginCollector(pluginCollector);
          break;
        default:
          throw DataXException.asDataXException(FrameworkErrorCode.ARGUMENT_ERROR,
              "Cant generateRunner for:" + pluginType);
      }

      newRunner.setTaskGroupId(taskGroupId);
      newRunner.setTaskId(this.taskId);
      newRunner.setRunnerCommunication(this.taskCommunication);

      return newRunner;
    }


    /**
     * 检查任务是否结束
     *
     * @return boolean
     */
    private boolean isTaskFinished() {
      // 如果reader 或 writer没有完成工作，那么直接返回工作没有完成
      if (readerThread.isAlive() || writerThread.isAlive()) {
        return false;
      }
      // 如果任务通讯类不空，或没结束，name返回工作没有完成（虽然read和write完成，但是通讯还没完成）
      if (taskCommunication == null || !taskCommunication.isFinished()) {
        return false;
      }
      return true;
    }

    private int getTaskId() {
      return taskId;
    }

    private long getTimeStamp() {
      return taskCommunication.getTimestamp();
    }

    private int getAttemptCount() {
      return attemptCount;
    }

    private boolean supportFailOver() {
      return writerRunner.supportFailOver();
    }

    private void shutdown() {
      writerRunner.shutdown();
      readerRunner.shutdown();
      if (writerThread.isAlive()) {
        writerThread.interrupt();
      }
      if (readerThread.isAlive()) {
        readerThread.interrupt();
      }
    }

    private boolean isShutdown() {
      return !readerThread.isAlive() && !writerThread.isAlive();
    }
  }
}
