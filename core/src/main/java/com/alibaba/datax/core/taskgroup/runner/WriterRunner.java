package com.alibaba.datax.core.taskgroup.runner;

import static com.alibaba.datax.common.statistics.PerfRecord.PHASE.WAIT_READ_TIME;
import static com.alibaba.datax.common.statistics.PerfRecord.PHASE.WRITE_TASK_DATA;
import static com.alibaba.datax.common.statistics.PerfRecord.PHASE.WRITE_TASK_DESTROY;
import static com.alibaba.datax.common.statistics.PerfRecord.PHASE.WRITE_TASK_INIT;
import static com.alibaba.datax.common.statistics.PerfRecord.PHASE.WRITE_TASK_POST;
import static com.alibaba.datax.common.statistics.PerfRecord.PHASE.WRITE_TASK_PREPARE;
import static com.alibaba.datax.core.statistics.communication.CommunicationTool.WAIT_READER_TIME;

import com.alibaba.datax.common.plugin.AbstractTaskPlugin;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jingxing on 14-9-1.
 * <p/>
 * 单个slice的writer执行调用
 */
public class WriterRunner extends AbstractRunner implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(WriterRunner.class);

  private RecordReceiver recordReceiver;

  public void setRecordReceiver(RecordReceiver receiver) {
    this.recordReceiver = receiver;
  }

  public WriterRunner(AbstractTaskPlugin abstractTaskPlugin) {
    super(abstractTaskPlugin);
  }

  @Override
  public void run() {
    Validate.isTrue(this.recordReceiver != null);

    Writer.Task taskWriter = (Writer.Task) this.getPlugin();
    int taskGroupId = getTaskGroupId();
    int taskId = getTaskId();
    //统计waitReadTime，并且在finally end
    PerfRecord channelWaitRead = new PerfRecord(taskGroupId, taskId, WAIT_READ_TIME);
    try {
      channelWaitRead.start();
      LOG.debug("task writer starts to do init ...");
      PerfRecord initPerfRecord = new PerfRecord(taskGroupId, taskId, WRITE_TASK_INIT);
      initPerfRecord.start();
      taskWriter.init();
      initPerfRecord.end();

      LOG.debug("task writer starts to do prepare ...");
      PerfRecord preparePerfRecord = new PerfRecord(taskGroupId, taskId, WRITE_TASK_PREPARE);
      preparePerfRecord.start();
      taskWriter.prepare();
      preparePerfRecord.end();
      LOG.debug("task writer starts to write ...");

      PerfRecord dataPerfRecord = new PerfRecord(taskGroupId, taskId, WRITE_TASK_DATA);
      dataPerfRecord.start();
      taskWriter.startWrite(recordReceiver);

      long count = CommunicationTool.getTotalReadRecords(super.getRunnerCommunication());
      dataPerfRecord.addCount(count);
      dataPerfRecord.addSize(CommunicationTool.getTotalReadBytes(super.getRunnerCommunication()));
      dataPerfRecord.end();

      LOG.debug("task writer starts to do post ...");
      PerfRecord postPerfRecord = new PerfRecord(taskGroupId, taskId, WRITE_TASK_POST);
      postPerfRecord.start();
      taskWriter.post();
      postPerfRecord.end();

      super.markSuccess();
    } catch (Throwable e) {
      LOG.error("Writer Runner Received Exceptions:", e);
      super.markFail(e);
    } finally {
      LOG.debug("task writer starts to do destroy ...");
      PerfRecord desPerfRecord = new PerfRecord(taskGroupId, taskId, WRITE_TASK_DESTROY);
      desPerfRecord.start();
      super.destroy();
      desPerfRecord.end();
      long elapsedTimeInNs = super.getRunnerCommunication().getLongCounter(WAIT_READER_TIME);
      channelWaitRead.end(elapsedTimeInNs);
    }
  }

  public boolean supportFailOver() {
    Writer.Task taskWriter = (Writer.Task) this.getPlugin();
    return taskWriter.supportFailOver();
  }

  @Override
  public void shutdown() {
    recordReceiver.shutdown();
  }
}
