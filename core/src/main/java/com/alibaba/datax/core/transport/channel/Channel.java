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
    
    // bps: bytes/s
    protected long byteSpeed;
    
    // tps: records/s
    protected long recordSpeed;
    
    protected long flowControlInterval;
    
    protected volatile boolean isClosed = false;
    
    protected Configuration conf;
    
    /**
     * 等待Reader处理完的时间，也就是pull的时间
     */
    protected volatile long waitReaderTime = 0;
    
    /**
     * 等待Writer处理完的时间，也就是push的时间
     */
    protected volatile long waitWriterTime = 0;
    
    private static Boolean isFirstPrint = true;
    
    private Communication currentCommunication;
    
    private Communication lastCommunication = new Communication();
    
    public Channel(final Configuration conf) {
        //channel的queue里默认record为1万条。原来为512条
        int capacity = conf.getInt(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CAPACITY, 2048);
        long byteSpeed = conf.getLong(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_SPEED_BYTE, 1024 * 1024);
        long recordSpeed = conf.getLong(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_SPEED_RECORD, 10000);
        
        if (capacity <= 0) {
            throw new IllegalArgumentException(String.format("通道容量[%d]必须大于0.", capacity));
        }
        
        synchronized (isFirstPrint) {
            if (isFirstPrint) {
                LOG.info("Channel set byte_speed_limit to " + byteSpeed + (byteSpeed <= 0 ? ", No bps activated."
                        : "."));
                LOG.info("Channel set record_speed_limit to " + recordSpeed + (recordSpeed <= 0 ? ", No tps activated."
                        : "."));
                isFirstPrint = false;
            }
        }
        
        this.taskGroupId = conf.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
        this.capacity = capacity;
        this.byteSpeed = byteSpeed;
        this.recordSpeed = recordSpeed;
        this.flowControlInterval = conf.getLong(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_FLOWCONTROLINTERVAL, 1000);
        //channel的queue默认大小为8M，原来为64M
        this.byteCapacity = conf.getInt(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CAPACITY_BYTE, 8 * 1024 * 1024);
        this.conf = conf;
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
        return this.conf;
    }
    
    public void setCommunication(final Communication communication) {
        this.currentCommunication = communication;
        this.lastCommunication.reset();
    }
    
    public void push(final Record r) {
        Validate.notNull(r, "record不能为空.");
        //子类MemoryChannel实现doPush
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
    
    /**
     * 对速度进行控制。它通过Communication记录总的写入数据大小和数据条数。然后每隔一段时间，检查速度。 如果速度过快，就会sleep一段时间，来把速度降下来。
     *
     * @param recordSize
     * @param byteSize
     */
    private void statPush(long recordSize, long byteSize) {
        // currentCommunication实时记录了Reader读取的总数据字节数和条数
        currentCommunication.increaseCounter(CommunicationTool.READ_SUCCEED_RECORDS, recordSize);
        currentCommunication.increaseCounter(CommunicationTool.READ_SUCCEED_BYTES, byteSize);
        
        //在读的时候进行统计waitCounter即可，因为写（pull）的时候可能正在阻塞，但读的时候已经能读到这个阻塞的counter数
        currentCommunication.setLongCounter(CommunicationTool.WAIT_READER_TIME, waitReaderTime);
        currentCommunication.setLongCounter(CommunicationTool.WAIT_WRITER_TIME, waitWriterTime);
        
        // 判断是否会限速
        boolean isChannelByteSpeedLimit = (this.byteSpeed > 0);
        boolean isChannelRecordSpeedLimit = (this.recordSpeed > 0);
        if (!isChannelByteSpeedLimit && !isChannelRecordSpeedLimit) {
            return;
        }
        
        // lastCommunication记录最后一次的时间
        long lastTimestamp = lastCommunication.getTimestamp();
        long nowTimestamp = System.currentTimeMillis();
        long interval = nowTimestamp - lastTimestamp;
        // 每隔flowControlInterval一段时间，就会检查是否超速
        if (interval - this.flowControlInterval >= 0) {
            long byteLimitSleepTime = 0;
            long recordLimitSleepTime = 0;
            if (isChannelByteSpeedLimit) {
                // 计算速度，(现在的字节数 - 上一次的字节数) / 过去的时间
                long currentByteSpeed = (CommunicationTool.getTotalReadBytes(currentCommunication) - CommunicationTool
                        .getTotalReadBytes(lastCommunication)) * 1000 / interval;
                if (currentByteSpeed > this.byteSpeed) {
                    // 计算根据byteLimit得到的休眠时间，
                    // 这段时间传输的字节数 / 期望的限定速度 - 这段时间
                    byteLimitSleepTime = currentByteSpeed * interval / this.byteSpeed - interval;
                }
            }
            
            if (isChannelRecordSpeedLimit) {
                long currentRecordSpeed =
                        (CommunicationTool.getTotalReadRecords(currentCommunication) - CommunicationTool
                                .getTotalReadRecords(lastCommunication)) * 1000 / interval;
                if (currentRecordSpeed > this.recordSpeed) {
                    // 计算根据recordLimit得到的休眠时间
                    recordLimitSleepTime = currentRecordSpeed * interval / this.recordSpeed - interval;
                }
            }
            
            // 休眠时间取较大值
            long sleepTime = byteLimitSleepTime < recordLimitSleepTime ? recordLimitSleepTime : byteLimitSleepTime;
            if (sleepTime > 0) {
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            // 保存读取字节数
            lastCommunication.setLongCounter(CommunicationTool.READ_SUCCEED_BYTES,
                    currentCommunication.getLongCounter(CommunicationTool.READ_SUCCEED_BYTES));
            // 保存读取失败的字节数
            lastCommunication.setLongCounter(CommunicationTool.READ_FAILED_BYTES,
                    currentCommunication.getLongCounter(CommunicationTool.READ_FAILED_BYTES));
            // 保存读取条数
            lastCommunication.setLongCounter(CommunicationTool.READ_SUCCEED_RECORDS,
                    currentCommunication.getLongCounter(CommunicationTool.READ_SUCCEED_RECORDS));
            // 保存读取失败的条数
            lastCommunication.setLongCounter(CommunicationTool.READ_FAILED_RECORDS,
                    currentCommunication.getLongCounter(CommunicationTool.READ_FAILED_RECORDS));
            // 记录保存的时间点
            lastCommunication.setTimestamp(nowTimestamp);
        }
    }
    
    /**
     * statPull方法，并没有限速。因为数据的整个流程是Reader -》 Channle -》 Writer， Reader的push速度限制了，Writer的pull速度也就没必要限速
     *
     * @param recordSize
     * @param byteSize
     */
    private void statPull(long recordSize, long byteSize) {
        currentCommunication.increaseCounter(CommunicationTool.WRITE_RECEIVED_RECORDS, recordSize);
        currentCommunication.increaseCounter(CommunicationTool.WRITE_RECEIVED_BYTES, byteSize);
    }
    
}
