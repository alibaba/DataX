package com.alibaba.datax.core.job.scheduler;

import static com.alibaba.datax.core.util.container.CoreConstant.DATAX_CORE_CONTAINER_JOB_REPORTINTERVAL;
import static com.alibaba.datax.core.util.container.CoreConstant.DATAX_CORE_CONTAINER_JOB_SLEEPINTERVAL;
import static com.alibaba.datax.core.util.container.CoreConstant.DATAX_JOB_CONTENT;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.util.ErrorRecordChecker;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 调度器
 */
public abstract class AbstractScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractScheduler.class);

  /**
   * 脏数据行数检查器，用于运行中随时检查脏数据是否超过限制（脏数据行数，或脏数据百分比）
   */
  private ErrorRecordChecker errorLimit;

  /**
   * 积累容器通讯器，来处理JobContainer、TaskGroupContainer和Task的通讯
   */
  private AbstractContainerCommunicator containerCommunicator;

  private Long jobId;

  public Long getJobId() {
    return jobId;
  }

  public AbstractScheduler(AbstractContainerCommunicator containerCommunicator) {
    this.containerCommunicator = containerCommunicator;
  }

  /**
   * 默认调度执行方法 <br>
   * 1 传入多个调度配置，获取报告时间+休息时间+jobId（赋值给全局jobId），生成错误记录检查类
   * 2 给全局jobId赋值，生成错误记录检查类，生成容器通讯类（反馈任务信息）
   * 3 根据入参计算task的数量，开始所有taskGroup
   *
   * @param cfgs List<Configuration>
   */
  public void schedule(List<Configuration> cfgs) {
    Validate.notNull(cfgs, "scheduler配置不能为空");
    int reportMillSec = cfgs.get(0).getInt(DATAX_CORE_CONTAINER_JOB_REPORTINTERVAL, 30000);
    int sleepMillSec = cfgs.get(0).getInt(DATAX_CORE_CONTAINER_JOB_SLEEPINTERVAL, 10000);

    this.jobId = cfgs.get(0).getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
    errorLimit = new ErrorRecordChecker(cfgs.get(0));
    //给 taskGroupContainer 的 Communication 注册
    this.containerCommunicator.registerCommunication(cfgs);
    int taskCnt = calculateTaskCount(cfgs);
    startAllTaskGroup(cfgs);
    Communication lastComm = new Communication();
    long lastReportTimeStamp = System.currentTimeMillis();
    try {
      while (true) {
        /**
         * step 1: collect job stat
         * step 2: getReport info, then report it
         * step 3: errorLimit do check
         * step 4: dealSucceedStat();
         * step 5: dealKillingStat();
         * step 6: dealFailedStat();
         * step 7: refresh last job stat, and then sleep for next while
         *
         * above steps, some ones should report info to DS
         *
         */
        Communication nowComm = this.containerCommunicator.collect();
        nowComm.setTimestamp(System.currentTimeMillis());
        LOG.debug(nowComm.toString());

        //汇报周期
        long now = System.currentTimeMillis();
        if (now - lastReportTimeStamp > reportMillSec) {
          Communication comm = CommunicationTool.getReportCommunication(nowComm, lastComm, taskCnt);

          this.containerCommunicator.report(comm);
          lastReportTimeStamp = now;
          lastComm = nowComm;
        }

        errorLimit.checkRecordLimit(nowComm);
        if (nowComm.getState() == State.SUCCEEDED) {
          LOG.info("Scheduler accomplished all tasks.");
          break;
        }

        if (isJobKilling(this.getJobId())) {
          dealKillingStat(this.containerCommunicator, taskCnt);
        } else if (nowComm.getState() == State.FAILED) {
          dealFailedStat(this.containerCommunicator, nowComm.getThrowable());
        }
        Thread.sleep(sleepMillSec);
      }
    } catch (InterruptedException e) {
      // 以 failed 状态退出
      LOG.error("捕获到InterruptedException异常!", e);
      throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, e);
    }
  }

  /**
   * 开始所有的taskGroup，只允许本包的类访问
   *
   * @param configurations List<Configuration>
   */
  protected abstract void startAllTaskGroup(List<Configuration> configurations);

  /**
   * 处理失败的状态
   *
   * @param frameworkCollector AbstractContainerCommunicator
   * @param throwable          Throwable
   */
  protected abstract void dealFailedStat(AbstractContainerCommunicator frameworkCollector,
      Throwable throwable);

  /**
   * 处理正在kill的状态
   *
   * @param frameworkCollector AbstractContainerCommunicator
   * @param totalTasks         int
   */
  protected abstract void dealKillingStat(AbstractContainerCommunicator frameworkCollector,
      int totalTasks);

  /**
   * 根据传入的配置列表，计算任务总数
   *
   * @param configurations List<Configuration>
   * @return int
   */
  private int calculateTaskCount(List<Configuration> configurations) {
    int totalTasks = 0;
    for (Configuration taskGroupCfg : configurations) {
      totalTasks += taskGroupCfg.getListConfiguration(DATAX_JOB_CONTENT).size();
    }
    return totalTasks;
  }

//    private boolean isJobKilling(Long jobId) {
//        Result<Integer> jobInfo = DataxServiceUtil.getJobInfo(jobId);
//        return jobInfo.getData() == State.KILLING.value();
//    }

  protected abstract boolean isJobKilling(Long jobId);
}
