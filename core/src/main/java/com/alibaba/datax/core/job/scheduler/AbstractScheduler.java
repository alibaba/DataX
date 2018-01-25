package com.alibaba.datax.core.job.scheduler;

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

public abstract class AbstractScheduler {
    private static final Logger LOG = LoggerFactory
            .getLogger(AbstractScheduler.class);

    private ErrorRecordChecker errorLimit;

    private AbstractContainerCommunicator containerCommunicator;

    private Long jobId;

    public Long getJobId() {
        return jobId;
    }

    public AbstractScheduler(AbstractContainerCommunicator containerCommunicator) {
        this.containerCommunicator = containerCommunicator;
    }

    public void schedule(List<Configuration> configurations) {
        Validate.notNull(configurations,
                "scheduler配置不能为空");
        int jobReportIntervalInMillSec = configurations.get(0).getInt(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_REPORTINTERVAL, 30000);
        int jobSleepIntervalInMillSec = configurations.get(0).getInt(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_SLEEPINTERVAL, 10000);

        this.jobId = configurations.get(0).getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);

        errorLimit = new ErrorRecordChecker(configurations.get(0));

        /**
         * 给 taskGroupContainer 的 Communication 注册
         */
        this.containerCommunicator.registerCommunication(configurations);

        int totalTasks = calculateTaskCount(configurations);
        startAllTaskGroup(configurations);

        Communication lastJobContainerCommunication = new Communication();

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
                Communication nowJobContainerCommunication = this.containerCommunicator.collect();
                nowJobContainerCommunication.setTimestamp(System.currentTimeMillis());
                LOG.debug(nowJobContainerCommunication.toString());

                //汇报周期
                long now = System.currentTimeMillis();
                if (now - lastReportTimeStamp > jobReportIntervalInMillSec) {
                    Communication reportCommunication = CommunicationTool
                            .getReportCommunication(nowJobContainerCommunication, lastJobContainerCommunication, totalTasks);

                    this.containerCommunicator.report(reportCommunication);
                    lastReportTimeStamp = now;
                    lastJobContainerCommunication = nowJobContainerCommunication;
                }

                errorLimit.checkRecordLimit(nowJobContainerCommunication);

                if (nowJobContainerCommunication.getState() == State.SUCCEEDED) {
                    LOG.info("Scheduler accomplished all tasks.");
                    break;
                }

                if (isJobKilling(this.getJobId())) {
                    dealKillingStat(this.containerCommunicator, totalTasks);
                } else if (nowJobContainerCommunication.getState() == State.FAILED) {
                    dealFailedStat(this.containerCommunicator, nowJobContainerCommunication.getThrowable());
                }

                Thread.sleep(jobSleepIntervalInMillSec);
            }
        } catch (InterruptedException e) {
            // 以 failed 状态退出
            LOG.error("捕获到InterruptedException异常!", e);

            throw DataXException.asDataXException(
                    FrameworkErrorCode.RUNTIME_ERROR, e);
        }

    }

    protected abstract void startAllTaskGroup(List<Configuration> configurations);

    protected abstract void dealFailedStat(AbstractContainerCommunicator frameworkCollector, Throwable throwable);

    protected abstract void dealKillingStat(AbstractContainerCommunicator frameworkCollector, int totalTasks);

    private int calculateTaskCount(List<Configuration> configurations) {
        int totalTasks = 0;
        for (Configuration taskGroupConfiguration : configurations) {
            totalTasks += taskGroupConfiguration.getListConfiguration(
                    CoreConstant.DATAX_JOB_CONTENT).size();
        }
        return totalTasks;
    }

//    private boolean isJobKilling(Long jobId) {
//        Result<Integer> jobInfo = DataxServiceUtil.getJobInfo(jobId);
//        return jobInfo.getData() == State.KILLING.value();
//    }

    protected  abstract  boolean isJobKilling(Long jobId);
}
