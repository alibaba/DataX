package com.alibaba.datax.plugin.reader.otsstreamreader.internal.core;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.Mode;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.OTSStreamReaderException;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.ShardCheckpoint;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.StreamJob;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.TimeUtils;
import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.openservices.ots.internal.streamclient.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class RecordProcessor implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(RecordProcessor.class);
    private static final long RECORD_CHECKPOINT_INTERVAL = 10 * TimeUtils.MINUTE_IN_MILLIS;

    private final SyncClientInterface ots;
    private final long startTimestampMillis;
    private final long endTimestampMillis;
    private final OTSStreamReaderConfig readerConfig;
    private boolean shouldSkip;
    private final CheckpointTimeTracker checkpointTimeTracker;
    private final RecordSender recordSender;
    private final boolean isExportSequenceInfo;
    private IStreamRecordSender otsStreamRecordSender;
    private long lastRecordCheckpointTime;

    private StreamJob stream;
    private StreamShard shard;
    private ShardCheckpoint startCheckpoint;

    // read state
    private String lastShardIterator;
    private String nextShardIterator;
    private long skipCount;

    // running state
    private long startTime;
    private long lastProcessTime;
    private AtomicBoolean stop;
    private AtomicLong sendRecordCount;

    public enum State {
        READY,        // initialized but not start
        RUNNING,      // start to read and process records
        SUCCEED,      // succeed to process all records
        FAILED,       // encounter exception and failed
        INTERRUPTED   // not finish but been interrupted
    }

    private State state;

    public RecordProcessor(SyncClientInterface ots,
                           OTSStreamReaderConfig config,
                           StreamJob stream,
                           StreamShard shardToProcess,
                           ShardCheckpoint startCheckpoint,
                           boolean shouldSkip,
                           CheckpointTimeTracker checkpointTimeTracker,
                           RecordSender recordSender) {
        this.ots = ots;
        this.readerConfig = config;
        this.stream = stream;
        this.shard = shardToProcess;
        this.startCheckpoint = startCheckpoint;
        this.startTimestampMillis = stream.getStartTimeInMillis();
        this.endTimestampMillis = stream.getEndTimeInMillis();
        this.shouldSkip = shouldSkip;
        this.checkpointTimeTracker = checkpointTimeTracker;
        this.recordSender = recordSender;
        this.isExportSequenceInfo = config.isExportSequenceInfo();
        this.lastRecordCheckpointTime = 0;

        // set init state
        startTime = 0;
        lastProcessTime = 0;
        state = State.READY;
        stop = new AtomicBoolean(true);
        sendRecordCount = new AtomicLong(0);
    }

    public StreamShard getShard() {
        return shard;
    }

    public State getState() {
        return state;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getLastProcessTime() {
        return lastProcessTime;
    }

    public void initialize() {
        if (readerConfig.getMode().equals(Mode.MULTI_VERSION)) {
            this.otsStreamRecordSender = new MultiVerModeRecordSender(recordSender, shard.getShardId(), isExportSequenceInfo);
        } else if (readerConfig.getMode().equals(Mode.SINGLE_VERSION_AND_UPDATE_ONLY)) {
            this.otsStreamRecordSender = new SingleVerAndUpOnlyModeRecordSender(recordSender, shard.getShardId(), isExportSequenceInfo, readerConfig.getColumns());
        } else {
            throw new OTSStreamReaderException("Internal Error. Unhandled Mode: " + readerConfig.getMode());
        }

        if (startCheckpoint.getCheckpoint().equals(CheckpointPosition.TRIM_HORIZON)) {
            lastShardIterator = null;
            nextShardIterator = ots.getShardIterator(new GetShardIteratorRequest(stream.getStreamId(), shard.getShardId())).getShardIterator();
            skipCount = startCheckpoint.getSkipCount();
        } else {
            lastShardIterator = null;
            nextShardIterator = startCheckpoint.getCheckpoint();
            skipCount = startCheckpoint.getSkipCount();
        }
        LOG.info("Initialize record processor. Mode: {}, StartCheckpoint: [{}], ShardId: {}, ShardIterator: {}, SkipCount: {}.",
                readerConfig.getMode(), startCheckpoint, shard.getShardId(), nextShardIterator, skipCount);
    }

    private long getTimestamp(StreamRecord record) {
        return record.getSequenceInfo().getTimestamp() / 1000;
    }

    void sendRecord(StreamRecord record) {
        sendRecordCount.incrementAndGet();
        otsStreamRecordSender.sendToDatax(record);
    }

    @Override
    public void run() {
        LOG.info("Start process records with startTime: {}, endTime: {}, nextShardIterator: {}, skipCount: {}.",
               startTimestampMillis, endTimestampMillis, nextShardIterator, skipCount);
        try {
            startTime = System.currentTimeMillis();
            lastProcessTime = startTime;
            boolean finished = false;

            stop.set(false);
            state = State.RUNNING;
            while (!stop.get()) {
                finished = readAndProcessRecords();
                lastProcessTime = System.currentTimeMillis();
                if (finished) {
                    break;
                }

                if (Thread.currentThread().isInterrupted()) {
                    state = State.INTERRUPTED;
                    break;
                }
            }

            if (finished) {
                state = State.SUCCEED;
            } else {
                state = State.INTERRUPTED;
            }
        } catch (Exception e) {
            LOG.error("Some fatal error has happened, shardId: {}, LastShardIterator: {}, NextShartIterator: {}.",
                    shard.getShardId(), lastShardIterator, nextShardIterator, e);
            state = State.FAILED;
        }
        LOG.info("Finished process records. ShardId: {}, RecordSent: {}.", shard.getShardId(), sendRecordCount.get());
    }

    public void stop() {
        stop.set(true);
    }

    /**
     * 处理所有记录。
     * 当发现已经获取得到完整的时间范围内的数据，则返回true，否则返回false。
     *
     * @param records
     * @param nextShardIterator
     * @return
     */
    boolean process(List<StreamRecord> records, String nextShardIterator) {
        if (records.isEmpty() && nextShardIterator != null) {
            LOG.info("ProcessFinished: No more data in shard, shardId: {}.", shard.getShardId());
            ShardCheckpoint checkpoint = new ShardCheckpoint(shard.getShardId(), stream.getVersion(), nextShardIterator, 0);
            checkpointTimeTracker.writeCheckpoint(endTimestampMillis, checkpoint, sendRecordCount.get());
            checkpointTimeTracker.setShardTimeCheckpoint(shard.getShardId(), endTimestampMillis, nextShardIterator);
            return true;
        }

        int size = records.size();

        // 只记录每次Iterator的第一个record作为checkpoint，因为checkpoint只记录shardIterator，而不记录skipCount。
        if (!records.isEmpty()) {
            long firstRecordTimestamp = getTimestamp(records.get(0));
            if (firstRecordTimestamp >= lastRecordCheckpointTime + RECORD_CHECKPOINT_INTERVAL) {
                lastRecordCheckpointTime = firstRecordTimestamp;
                checkpointTimeTracker.setShardTimeCheckpoint(shard.getShardId(), firstRecordTimestamp, lastShardIterator);
            }
        }

        for (int i = 0; i < size; i++) {
            long timestamp = getTimestamp(records.get(i));
            LOG.debug("Process record with timestamp: {}.", timestamp);
            if (timestamp < endTimestampMillis) {
                if (shouldSkip && (timestamp < startTimestampMillis)) {
                    LOG.debug("Skip record out of start time: {}, startTime: {}.", timestamp, startTimestampMillis);
                    continue;
                }
                shouldSkip = false;
                if (skipCount > 0) {
                    LOG.debug("Skip record. Timestamp: {}, SkipCount: {}.", timestamp, skipCount);
                    skipCount -= 1;
                    continue;
                }

                LOG.debug("Send record. Timestamp: {}.", timestamp);
                sendRecord(records.get(i));
            } else {
                LOG.info("ProcessFinished: Record in shard reach boundary of endTime, shardId: {}. Timestamp: {}, EndTime: {}", shard.getShardId(), timestamp, endTimestampMillis);
                ShardCheckpoint checkpoint = new ShardCheckpoint(shard.getShardId(), stream.getVersion(), lastShardIterator, i);
                checkpointTimeTracker.writeCheckpoint(endTimestampMillis, checkpoint, sendRecordCount.get());
                return true;
            }
        }

        if (nextShardIterator == null) {
            LOG.info("ProcessFinished: Shard has reach to end, shardId: {}.", shard.getShardId());
            ShardCheckpoint checkpoint = new ShardCheckpoint(shard.getShardId(), stream.getVersion(), CheckpointPosition.SHARD_END, 0);
            checkpointTimeTracker.writeCheckpoint(endTimestampMillis, checkpoint, sendRecordCount.get());
            return true;
        }

        return false;
    }

    private boolean readAndProcessRecords() {
        LOG.debug("Read and process records. ShardId: {}, ShardIterator: {}.", shard.getShardId(), nextShardIterator);
        GetStreamRecordRequest request = new GetStreamRecordRequest(nextShardIterator);
        GetStreamRecordResponse response = ots.getStreamRecord(request);
        lastShardIterator = nextShardIterator;
        nextShardIterator = response.getNextShardIterator();
        return processRecords(response.getRecords(), nextShardIterator);
    }

    public boolean processRecords(List<StreamRecord> records, String nextShardIterator) {
        long startTime = System.currentTimeMillis();

        if (records.isEmpty()) {
            LOG.info("StartProcessRecords: size: {}.", records.size());
        } else {
            LOG.debug("StartProcessRecords: size: {}, recordTime: {}.", records.size(), getTimestamp(records.get(0)));
        }

        if (process(records, nextShardIterator)) {
            return true;
        }

        LOG.debug("ProcessRecords, ProcessShard:{}, ProcessTime: {}, Size:{}, NextShardIterator:{}",
                shard.getShardId(), System.currentTimeMillis() - startTime, records.size(), nextShardIterator);
        return false;
    }
}
