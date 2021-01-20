package com.alibaba.datax.core.statistics.container.communicator.job;

import static com.alibaba.datax.core.util.container.CoreConstant.DATAX_CORE_CONTAINER_JOB_ID;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.container.collector.ProcessInnerCollector;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.statistics.container.report.ProcessInnerReporter;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 主要处理JobContainer和TaskGroupContainer之间的信息传递
 */
public class StandAloneJobContainerCommunicator extends AbstractContainerCommunicator {

  private static final Logger LOG = LoggerFactory
      .getLogger(StandAloneJobContainerCommunicator.class);

  public StandAloneJobContainerCommunicator(Configuration cfg) {
    super(cfg);
    super.setCollector(new ProcessInnerCollector(cfg.getLong(DATAX_CORE_CONTAINER_JOB_ID)));
    super.setReporter(new ProcessInnerReporter());
  }

  @Override
  public void registerCommunication(List<Configuration> configurationList) {
    super.getCollector().registerTGCommunication(configurationList);
  }

  /**
   * JobContainer每隔一段时间 主动 获取TaskGroup的信息。最后调用本类的#report向上级汇报，
   * 这里JobContainer已经是最上级了，向日志中输出先关信息即可
   * @return
   */
  @Override
  public Communication collect() {
    return super.getCollector().collectFromTaskGroup();
  }

  @Override
  public State collectState() {
    return this.collect().getState();
  }

  /**
   * 和 DistributeJobContainerCollector 的 report 实现一样
   * 每隔一段时间向JobContainer 主动 发送自己的状态
   */
  @Override
  public void report(Communication communication) {
    super.getReporter().reportJobCommunication(super.getJobId(), communication);
    LOG.info(CommunicationTool.Stringify.getSnapshot(communication));
    reportVmInfo();
  }

  @Override
  public Communication getCommunication(Integer taskGroupId) {
    return super.getCollector().getTGCommunication(taskGroupId);
  }

  @Override
  public Map<Integer, Communication> getCommunicationMap() {
    return super.getCollector().getTGCommunicationMap();
  }
}
