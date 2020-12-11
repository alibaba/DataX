package com.alibaba.datax.core.taskgroup.runner;

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

    private static final Logger LOG = LoggerFactory
            .getLogger(ReaderRunner.class);

    private RecordSender recordSender;

    public void setRecordSender(RecordSender recordSender) {
        this.recordSender = recordSender;
    }

    public ReaderRunner(AbstractTaskPlugin abstractTaskPlugin) {
        super(abstractTaskPlugin);
    }

    @Override
    public void run() {
        assert null != this.recordSender;

        Reader.Task taskReader = (Reader.Task) this.getPlugin();

        //统计waitWriterTime，并且在finally才end。
        PerfRecord channelWaitWrite = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.WAIT_WRITE_TIME);
        try {
            channelWaitWrite.start();

            LOG.debug("task reader starts to do init ...");
            PerfRecord initPerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.READ_TASK_INIT);
            initPerfRecord.start();
            taskReader.init();
            initPerfRecord.end();

            LOG.debug("task reader starts to do prepare ...");
            PerfRecord preparePerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.READ_TASK_PREPARE);
            preparePerfRecord.start();
            taskReader.prepare();
            preparePerfRecord.end();

            LOG.debug("task reader starts to read ...");
            PerfRecord dataPerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.READ_TASK_DATA);
            dataPerfRecord.start();
            taskReader.startRead(recordSender);
            recordSender.terminate();

            dataPerfRecord.addCount(CommunicationTool.getTotalReadRecords(super.getRunnerCommunication()));
            dataPerfRecord.addSize(CommunicationTool.getTotalReadBytes(super.getRunnerCommunication()));
            dataPerfRecord.end();

            LOG.debug("task reader starts to do post ...");
            PerfRecord postPerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.READ_TASK_POST);
            postPerfRecord.start();
            taskReader.post();
            postPerfRecord.end();
            // automatic flush
            // super.markSuccess(); 这里不能标记为成功，成功的标志由 writerRunner 来标志（否则可能导致 reader 先结束，而 writer 还没有结束的严重 bug）
        } catch (Throwable e) {
            LOG.error("Reader runner Received Exceptions:", e);
            super.markFail(e);
        } finally {
            LOG.debug("task reader starts to do destroy ...");
            PerfRecord desPerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.READ_TASK_DESTROY);
            desPerfRecord.start();
            super.destroy();
            desPerfRecord.end();

            channelWaitWrite.end(super.getRunnerCommunication().getLongCounter(CommunicationTool.WAIT_WRITER_TIME));

            long transformerUsedTime = super.getRunnerCommunication().getLongCounter(CommunicationTool.TRANSFORMER_USED_TIME);
            if (transformerUsedTime > 0) {
                PerfRecord transformerRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.TRANSFORMER_TIME);
                transformerRecord.start();
                transformerRecord.end(transformerUsedTime);
            }
        }
    }

    public void shutdown(){
        recordSender.shutdown();
    }
}
