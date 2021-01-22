package com.alibaba.datax.core.taskgroup.runner;

import static com.alibaba.datax.common.statistics.PerfRecord.PHASE.READ_TASK_DATA;
import static com.alibaba.datax.common.statistics.PerfRecord.PHASE.READ_TASK_DESTROY;
import static com.alibaba.datax.common.statistics.PerfRecord.PHASE.READ_TASK_INIT;
import static com.alibaba.datax.common.statistics.PerfRecord.PHASE.READ_TASK_POST;
import static com.alibaba.datax.common.statistics.PerfRecord.PHASE.READ_TASK_PREPARE;
import static com.alibaba.datax.common.statistics.PerfRecord.PHASE.TRANSFORMER_TIME;
import static com.alibaba.datax.common.statistics.PerfRecord.PHASE.WAIT_WRITE_TIME;
import static com.alibaba.datax.core.statistics.communication.CommunicationTool.TRANSFORMER_USED_TIME;
import static com.alibaba.datax.core.statistics.communication.CommunicationTool.WAIT_WRITER_TIME;

import com.alibaba.datax.common.plugin.AbstractTaskPlugin;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jingxing on 14-9-1.
 * <p/>
 * 单个slice的reader执行调用
 */
public class ReaderRunner extends AbstractRunner implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ReaderRunner.class);

  private RecordSender recordSender;

  public void setRecordSender(RecordSender recordSender) {
    this.recordSender = recordSender;
  }

  public ReaderRunner(AbstractTaskPlugin abstractTaskPlugin) {
    super(abstractTaskPlugin);
  }

  /**
   * 具体执行的方法 <br>
   * 1 reader.task每个会执行4个阶段，分别是  init()、prepare()、startRead(recordSender)、post(); <br>
   * 2 reader.task的每个执行阶段，会收集该阶段的 信息保存到PerfRecord中，PerfRecord.start 方法会将信息汇总到PerfTrace <br/>
   */
  @Override
  public void run() {
    assert null != this.recordSender;
    //将当前插件类型强转为Reader下的Task
    Reader.Task taskReader = (Reader.Task) this.getPlugin();
    int taskGroupId = getTaskGroupId();
    int taskId = getTaskId();
    //统计waitWriterTime，并且在finally才end。
    PerfRecord channelWaitWrite = new PerfRecord(taskGroupId, taskId, WAIT_WRITE_TIME);
    try {
      channelWaitWrite.start();

      LOG.debug("task reader starts to do init ...");
      PerfRecord initPerfRecord = new PerfRecord(taskGroupId, taskId, READ_TASK_INIT);
      initPerfRecord.start();
      taskReader.init();
      initPerfRecord.end();

      LOG.debug("task reader starts to do prepare ...");
      PerfRecord preparePerfRecord = new PerfRecord(taskGroupId, taskId, READ_TASK_PREPARE);
      preparePerfRecord.start();
      taskReader.prepare();
      preparePerfRecord.end();

      LOG.debug("task reader starts to read ...");
      PerfRecord dataPerfRecord = new PerfRecord(taskGroupId, taskId, READ_TASK_DATA);
      dataPerfRecord.start();
      taskReader.startRead(recordSender);
      recordSender.terminate();

      long count = CommunicationTool.getTotalReadRecords(super.getRunnerCommunication());
      dataPerfRecord.addCount(count);
      dataPerfRecord.addSize(CommunicationTool.getTotalReadBytes(super.getRunnerCommunication()));
      dataPerfRecord.end();

      LOG.debug("task reader starts to do post ...");
      PerfRecord postPerfRecord = new PerfRecord(taskGroupId, taskId, READ_TASK_POST);
      postPerfRecord.start();
      taskReader.post();
      postPerfRecord.end();
      // automatic flush
      // super.markSuccess(); 这里不能标记为成功，成功的标志由 writerRunner 来标志（否则可能导致 reader 先结束，
      // 而 writer 还没有结束的严重 bug）
    } catch (Throwable e) {
      LOG.error("Reader runner Received Exceptions:", e);
      super.markFail(e);
    } finally {
      LOG.debug("task reader starts to do destroy ...");
      PerfRecord desPerfRecord = new PerfRecord(taskGroupId, taskId, READ_TASK_DESTROY);
      desPerfRecord.start();
      super.destroy();
      desPerfRecord.end();

      long elapsedTimeInNs = super.getRunnerCommunication().getLongCounter(WAIT_WRITER_TIME);
      channelWaitWrite.end(elapsedTimeInNs);

      long transformUsedTime = super.getRunnerCommunication().getLongCounter(TRANSFORMER_USED_TIME);
      if (transformUsedTime > 0) {
        PerfRecord transformerRecord = new PerfRecord(taskGroupId, taskId, TRANSFORMER_TIME);
        transformerRecord.start();
        transformerRecord.end(transformUsedTime);
      }
    }
  }

  @Override
  public void shutdown() {
    recordSender.shutdown();
  }
}
