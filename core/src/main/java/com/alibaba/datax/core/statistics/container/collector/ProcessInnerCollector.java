package com.alibaba.datax.core.statistics.container.collector;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;

public class ProcessInnerCollector extends AbstractCollector {

    public ProcessInnerCollector(Long jobId) {
        super.setJobId(jobId);
    }

    @Override
    public Communication collectFromTaskGroup() {
        /**
         * 在整合到JAVA中启动时会使多个Jobcontiner 使用同一个LocalTGCommunicationManager.taskGroupCommunicationMap
         * taskGroupCommunicationMap中存放当前Job的分组情况一般KEY从0开始
         * 因为是静态的共享的每个JobContiner的taskgroup都是从0开始这样就到导致新的jobcontiner的收集器会收集的老的jobcontiner中的统计信息
         * 这里通过传指定的JobId来区分统计信息
         */
        return LocalTGCommunicationManager.getJobCommunication(this.getJobId());
    }

}
