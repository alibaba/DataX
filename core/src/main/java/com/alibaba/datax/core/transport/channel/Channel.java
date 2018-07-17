package com.alibaba.datax.core.transport.channel;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.transport.record.TerminateRecord;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Created by jingxing on 14-8-25.
 * <p/>
 * 统计和限速都在这里
 */
public abstract class Channel {

    private static final Logger LOG = LoggerFactory.getLogger(Channel.class);

    protected int taskGroupId;

    protected int capacity;

    protected int byteCapacity;

    protected long byteSpeed; // bps: bytes/s

    protected long recordSpeed; // tps: records/s

    protected long flowControlInterval;

    protected volatile boolean isClosed = false;

    protected Configuration configuration = null;

    protected volatile long waitReaderTime = 0;

    protected volatile long waitWriterTime = 0;

    private static Boolean isFirstPrint = true;

    private Communication currentCommunication;

    private Communication lastCommunication = new Communication();

    public Channel(final Configuration configuration) {
        //channel的queue里默认record为1万条。原来为512条
        int capacity = configuration.getInt(
                CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CAPACITY, 2048);
        long byteSpeed = configuration.getLong(
                CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_SPEED_BYTE, 1024 * 1024);
        long recordSpeed = configuration.getLong(
                CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_SPEED_RECORD, 10000);

        if (capacity <= 0) {
            throw new IllegalArgumentException(String.format(
                    "通道容量[%d]必须大于0.", capacity));
        }

        synchronized (isFirstPrint) {
            if (isFirstPrint) {
                Channel.LOG.info("Channel set byte_speed_limit to " + byteSpeed
                        + (byteSpeed <= 0 ? ", No bps activated." : "."));
                Channel.LOG.info("Channel set record_speed_limit to " + recordSpeed
                        + (recordSpeed <= 0 ? ", No tps activated." : "."));
                isFirstPrint = false;
            }
        }

        this.taskGroupId = configuration.getInt(
                CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
        this.capacity = capacity;
        this.byteSpeed = byteSpeed;
        this.recordSpeed = recordSpeed;
        this.flowControlInterval = configuration.getLong(
                CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_FLOWCONTROLINTERVAL, 1000);
        //channel的queue默认大小为8M，原来为64M
        this.byteCapacity = configuration.getInt(
                CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CAPACITY_BYTE, 8 * 1024 * 1024);
        this.configuration = configuration;
    }

    public void close() {
        this.isClosed = true;
    }

    public void open() {
        this.isClosed = false;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public int getTaskGroupId() {
        return this.taskGroupId;
    }

    public int getCapacity() {
        return capacity;
    }

    public long getByteSpeed() {
        return byteSpeed;
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

    public void setCommunication(final Communication communication) {
        this.currentCommunication = communication;
        this.lastCommunication.reset();
    }

    public void push(final Record r) {
        Validate.notNull(r, "record不能为空.");
        this.doPush(r);
        this.statPush(1L, r.getByteSize());
    }

    public void pushTerminate(final TerminateRecord r) {
        Validate.notNull(r, "record不能为空.");
        this.doPush(r);

//        // 对 stage + 1
//        currentCommunication.setLongCounter(CommunicationTool.STAGE,
//                currentCommunication.getLongCounter(CommunicationTool.STAGE) + 1);
    }

    public void pushAll(final Collection<Record> rs) {
        Validate.notNull(rs);
        Validate.noNullElements(rs);
        this.doPushAll(rs);
        this.statPush(rs.size(), this.getByteSize(rs));
    }

    public Record pull() {
        Record record = this.doPull();
        this.statPull(1L, record.getByteSize());
        return record;
    }

    public void pullAll(final Collection<Record> rs) {
        Validate.notNull(rs);
        this.doPullAll(rs);
        this.statPull(rs.size(), this.getByteSize(rs));
    }

    protected abstract void doPush(Record r);

    protected abstract void doPushAll(Collection<Record> rs);

    protected abstract Record doPull();

    protected abstract void doPullAll(Collection<Record> rs);

    public abstract int size();

    public abstract boolean isEmpty();

    public abstract void clear();

    private long getByteSize(final Collection<Record> rs) {
        long size = 0;
        for (final Record each : rs) {
            size += each.getByteSize();
        }
        return size;
    }

    private void statPush(long recordSize, long byteSize) {
        currentCommunication.increaseCounter(CommunicationTool.READ_SUCCEED_RECORDS,
                recordSize);
        currentCommunication.increaseCounter(CommunicationTool.READ_SUCCEED_BYTES,
                byteSize);
        //在读的时候进行统计waitCounter即可，因为写（pull）的时候可能正在阻塞，但读的时候已经能读到这个阻塞的counter数

        currentCommunication.setLongCounter(CommunicationTool.WAIT_READER_TIME, waitReaderTime);
        currentCommunication.setLongCounter(CommunicationTool.WAIT_WRITER_TIME, waitWriterTime);

        boolean isChannelByteSpeedLimit = (this.byteSpeed > 0);
        boolean isChannelRecordSpeedLimit = (this.recordSpeed > 0);
        if (!isChannelByteSpeedLimit && !isChannelRecordSpeedLimit) {
            return;
        }

        long lastTimestamp = lastCommunication.getTimestamp();
        long nowTimestamp = System.currentTimeMillis();
        long interval = nowTimestamp - lastTimestamp;
        if (interval - this.flowControlInterval >= 0) {
            long byteLimitSleepTime = 0;
            long recordLimitSleepTime = 0;
            if (isChannelByteSpeedLimit) {
                long currentByteSpeed = (CommunicationTool.getTotalReadBytes(currentCommunication) -
                        CommunicationTool.getTotalReadBytes(lastCommunication)) * 1000 / interval;
                if (currentByteSpeed > this.byteSpeed) {
                    // 计算根据byteLimit得到的休眠时间
                    byteLimitSleepTime = currentByteSpeed * interval / this.byteSpeed
                            - interval;
                }
            }

            if (isChannelRecordSpeedLimit) {
                long currentRecordSpeed = (CommunicationTool.getTotalReadRecords(currentCommunication) -
                        CommunicationTool.getTotalReadRecords(lastCommunication)) * 1000 / interval;
                if (currentRecordSpeed > this.recordSpeed) {
                    // 计算根据recordLimit得到的休眠时间
                    recordLimitSleepTime = currentRecordSpeed * interval / this.recordSpeed
                            - interval;
                }
            }

            // 休眠时间取较大值
            long sleepTime = byteLimitSleepTime < recordLimitSleepTime ?
                    recordLimitSleepTime : byteLimitSleepTime;
            if (sleepTime > 0) {
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            lastCommunication.setLongCounter(CommunicationTool.READ_SUCCEED_BYTES,
                    currentCommunication.getLongCounter(CommunicationTool.READ_SUCCEED_BYTES));
            lastCommunication.setLongCounter(CommunicationTool.READ_FAILED_BYTES,
                    currentCommunication.getLongCounter(CommunicationTool.READ_FAILED_BYTES));
            lastCommunication.setLongCounter(CommunicationTool.READ_SUCCEED_RECORDS,
                    currentCommunication.getLongCounter(CommunicationTool.READ_SUCCEED_RECORDS));
            lastCommunication.setLongCounter(CommunicationTool.READ_FAILED_RECORDS,
                    currentCommunication.getLongCounter(CommunicationTool.READ_FAILED_RECORDS));
            lastCommunication.setTimestamp(nowTimestamp);
        }
    }

    private void statPull(long recordSize, long byteSize) {
        currentCommunication.increaseCounter(
                CommunicationTool.WRITE_RECEIVED_RECORDS, recordSize);
        currentCommunication.increaseCounter(
                CommunicationTool.WRITE_RECEIVED_BYTES, byteSize);
    }

}
