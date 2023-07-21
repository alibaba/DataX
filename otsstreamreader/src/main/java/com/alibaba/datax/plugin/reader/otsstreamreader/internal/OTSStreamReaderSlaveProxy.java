package com.alibaba.datax.plugin.reader.otsstreamreader.internal;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConstants;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.core.*;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.ShardCheckpoint;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.StreamJob;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.OTSHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.TimeUtils;
import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.openservices.ots.internal.streamclient.model.CheckpointPosition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class OTSStreamReaderSlaveProxy {
    private static final Logger LOG = LoggerFactory.getLogger(OTSStreamReaderSlaveProxy.class);
    private static AtomicInteger slaveNumber = new AtomicInteger(0);

    private OTSStreamReaderConfig config;
    private SyncClientInterface ots;
    private Map<String, ShardCheckpoint> shardToCheckpointMap = new ConcurrentHashMap<String, ShardCheckpoint>();
    private CheckpointTimeTracker checkpointInfoTracker;
    private OTSStreamReaderChecker checker;

    private StreamJob streamJob;
    private Map<String, StreamShard> allShardsMap; // all shards from job master
    private Map<String, StreamShard> ownedShards; // shards to read arranged by job master
    private boolean findCheckpoints; // whether find checkpoint for last job, if so, we should read from checkpoint and skip nothing.
    private String slaveId = UUID.randomUUID().toString();
    private StreamDetails streamDetails;
    private boolean enableSeekIteratorByTimestamp;

    public void init(final OTSStreamReaderConfig otsStreamReaderConfig, StreamJob streamJob, List<StreamShard> allShards, Set<String> ownedShardIds) {
        slaveNumber.getAndIncrement();
        this.config = otsStreamReaderConfig;
        this.ots = OTSHelper.getOTSInstance(config);
        this.streamJob = streamJob;
        this.streamDetails = OTSHelper.getStreamDetails(ots, this.streamJob.getTableName(),config.isTimeseriesTable());
        this.checkpointInfoTracker = new CheckpointTimeTracker(ots, config.getStatusTable(), this.streamJob.getStreamId());
        this.checker = new OTSStreamReaderChecker(ots, config);
        this.allShardsMap = OTSHelper.toShardMap(allShards);
        this.enableSeekIteratorByTimestamp = otsStreamReaderConfig.getEnableSeekIteratorByTimestamp();

        LOG.info("SlaveId: {}, ShardIds: {}, OwnedShards: {}.", slaveId, allShards, ownedShardIds);
        this.ownedShards = new HashMap<String, StreamShard>();
        for (String ownedShardId : ownedShardIds) {
            ownedShards.put(ownedShardId, allShardsMap.get(ownedShardId));
        }

        for (String shardId : this.streamJob.getShardIds()) {
            shardToCheckpointMap.put(shardId, new ShardCheckpoint(shardId, this.streamJob.getVersion(), CheckpointPosition.TRIM_HORIZON, 0));
        }

        findCheckpoints = checker.checkAndSetCheckpoints(checkpointInfoTracker, allShardsMap, streamJob, shardToCheckpointMap);
        if (!findCheckpoints && !enableSeekIteratorByTimestamp) {
            LOG.info("Checkpoint for stream '{}' in timestamp '{}' is not found. EnableSeekIteratorByTimestamp: {}", streamJob.getStreamId(), streamJob.getStartTimeInMillis(), this.enableSeekIteratorByTimestamp);
            setWithNearestCheckpoint();
        }

        LOG.info("Find checkpoints: {}, EnableSeekIteratorByTimestamp: {}", findCheckpoints, enableSeekIteratorByTimestamp);
        for (Map.Entry<String, StreamShard> shard : ownedShards.entrySet()) {
            LOG.info("Shard to process, ShardInfo: [{}], StartCheckpoint: [{}].", shard.getValue(), shardToCheckpointMap.get(shard.getKey()));
        }
        LOG.info("Count of owned shards: {}. ShardIds: {}.", ownedShardIds.size(), ownedShardIds);
    }

    public boolean isFindCheckpoints() {
        return findCheckpoints;
    }

    public Map<String, StreamShard> getAllShardsMap() {
        return allShardsMap;
    }

    public Map<String, StreamShard> getOwnedShards() {
        return ownedShards;
    }

    public Map<String, ShardCheckpoint> getShardToCheckpointMap() {
        return shardToCheckpointMap;
    }

    /**
     * 没有找到上一次任务的checkpoint，需要重新从头开始读。
     * 为了减少扫描的数据量，尝试查找里startTime最近的一次checkpoint。
     */
    private void setWithNearestCheckpoint() {
        long expirationTime = (streamDetails.getExpirationTime() - 1) * TimeUtils.HOUR_IN_MILLIS;
        long timeRangeBegin = System.currentTimeMillis() - expirationTime;
        long timeRangeEnd = this.config.getStartTimestampMillis() - 1;
        if (timeRangeBegin < timeRangeEnd) {
            for (String shardId : ownedShards.keySet()) {
                LOG.info("Try find nearest checkpoint for shard {}, startTime: {}.", shardId, config.getStartTimestampMillis());
                String checkpoint = this.checkpointInfoTracker.getShardLargestCheckpointInTimeRange(shardId, timeRangeBegin, timeRangeEnd);
                if (checkpoint != null) {
                    LOG.info("Found checkpoint for shard {}, checkpoint: {}.", shardId, checkpoint);
                    shardToCheckpointMap.put(shardId, new ShardCheckpoint(shardId, streamJob.getVersion(), checkpoint, 0));
                }
            }
        }
    }

    private int calcThreadPoolSize() {
        int threadNum = 0;
        // 如果配置了thread num，则计算平均每个slave所启动的thread的个数
        if (config.getThreadNum() > 0) {
            threadNum = config.getThreadNum() / slaveNumber.get();
        } else {
            threadNum = Runtime.getRuntime().availableProcessors() * 4 / slaveNumber.get();
        }

        if (threadNum == 0) {
            threadNum = 1;
        }
        LOG.info("ThreadNum: {}.", threadNum);
        return threadNum;
    }

    private Map<String, StreamShard> filterShardsReachEnd(Map<String, StreamShard> ownedShards, Map<String, ShardCheckpoint> allCheckpoints) {
        Map<String, StreamShard> allShardToProcess = new HashMap<String, StreamShard>();
        for (Map.Entry<String, StreamShard> shard : ownedShards.entrySet()) {
            String shardId = shard.getKey();
            if (allCheckpoints.get(shardId).getCheckpoint().equals(CheckpointPosition.SHARD_END)) {
                LOG.info("Shard has reach end, no need to process. ShardId: {}.", shardId);
                // but we need to set checkpoint for this job
                checkpointInfoTracker.writeCheckpoint(streamJob.getEndTimeInMillis(),
                        new ShardCheckpoint(shardId, streamJob.getVersion(), CheckpointPosition.SHARD_END, 0), 0);
            } else {
                allShardToProcess.put(shard.getKey(), shard.getValue());
            }
        }
        return allShardToProcess;
    }

    public void startRead(RecordSender recordSender) {
        int threadPoolSize = calcThreadPoolSize();
        ExecutorService executorService = new ThreadPoolExecutor(
                0, threadPoolSize, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(ownedShards.size()));
        LOG.info("Start thread pool with size: {}, ShardsCount: {}, SlaveCount: {}.", threadPoolSize, ownedShards.size(), slaveNumber.get());
        try {
            Map<String, StreamShard> allShardToProcess = filterShardsReachEnd(ownedShards, shardToCheckpointMap);
            Map<String, ShardStatusChecker.ProcessState> shardProcessingState = new HashMap<String, ShardStatusChecker.ProcessState>();
            for (String shardId : allShardToProcess.keySet()) {
                shardProcessingState.put(shardId, ShardStatusChecker.ProcessState.BLOCK);
            }

            List<RecordProcessor> processors = new ArrayList<RecordProcessor>();

            // 获取当前所有shard的checkpoint状态，对当前的owned shard执行对应的任务。
            long lastLogTime = System.currentTimeMillis();
            while (!allShardToProcess.isEmpty()) {
                Map<String, ShardCheckpoint> checkpointMap = checkpointInfoTracker.getAllCheckpoints(streamJob.getEndTimeInMillis());

                // 检查当前job的checkpoint，排查是否有其他job误入或者出现不明的shard。
                checkCheckpoint(checkpointMap, streamJob);

                // 找到需要处理的shard以及确定不需要被处理的shard
                List<StreamShard> shardToProcess = new ArrayList<StreamShard>();
                List<StreamShard> shardNoNeedProcess = new ArrayList<StreamShard>();
                List<StreamShard> shardBlocked = new ArrayList<StreamShard>();
                ShardStatusChecker.findShardToProcess(allShardToProcess, allShardsMap, checkpointMap, shardToProcess, shardNoNeedProcess, shardBlocked);

                // 将不需要处理的shard，设置checkpoint，代表本轮处理完毕，且checkpoint为TRIM_HORIZON
                for (StreamShard shard : shardNoNeedProcess) {
                    LOG.info("Skip shard: {}.", shard.getShardId());
                    ShardCheckpoint checkpoint = new ShardCheckpoint(shard.getShardId(), streamJob.getVersion(), CheckpointPosition.TRIM_HORIZON, 0);
                    checkpointInfoTracker.writeCheckpoint(config.getEndTimestampMillis(), checkpoint, 0);
                    shardProcessingState.put(shard.getShardId(), ShardStatusChecker.ProcessState.SKIP);
                }

                for (StreamShard shard : shardToProcess) {
                    RecordProcessor processor = new RecordProcessor(ots, config, streamJob, shard,
                            shardToCheckpointMap.get(shard.getShardId()), !findCheckpoints, checkpointInfoTracker, recordSender);
                    processor.initialize();
                    executorService.submit(processor);
                    processors.add(processor);
                    shardProcessingState.put(shard.getShardId(), ShardStatusChecker.ProcessState.READY);
                }

                // 等待所有任务执行完毕，并且检查每个任务的状态，检查是否发生hang或长时间没有数据
                checkProcessorRunningStatus(processors);

                if (!allShardToProcess.isEmpty()) {
                    TimeUtils.sleepMillis(config.getSlaveLoopInterval());
                }

                long now = System.currentTimeMillis();
                if (now - lastLogTime > config.getSlaveLoggingStatusInterval()) {
                    logShardProcessingState(shardProcessingState);
                    LOG.info("AllCheckpoints: {}", checkpointMap);
                    lastLogTime = now;
                }
            }

            LOG.info("All shard is processing.");
            logShardProcessingState(shardProcessingState);
            // 等待当前分配的shard的读取任务执行完毕后退出。
            while (true) {
                boolean finished = true;
                checkProcessorRunningStatus(processors);
                for (RecordProcessor processor : processors) {
                    RecordProcessor.State state = processor.getState();
                    if (state != RecordProcessor.State.SUCCEED) {
                        LOG.info("Shard is processing, shardId: {}, status: {}.", processor.getShard().getShardId(), state);
                        finished = false;
                    }
                }

                if (finished) {
                    LOG.info("All record processor finished.");
                    break;
                }

                TimeUtils.sleepMillis(config.getSlaveLoopInterval());
            }

        } catch (TableStoreException ex) {
            throw DataXException.asDataXException(new OTSReaderError(ex.getErrorCode(), "SyncClientInterface Error"), ex.toString(), ex);
        } catch (OTSStreamReaderException ex) {
            LOG.error("SlaveId: {}, OwnedShards: {}.", slaveId, ownedShards, ex);
            throw DataXException.asDataXException(OTSReaderError.ERROR, ex.toString(), ex);
        } catch (Exception ex) {
            LOG.error("SlaveId: {}, OwnedShards: {}.", slaveId, ownedShards, ex);
            throw DataXException.asDataXException(OTSReaderError.ERROR, ex.toString(), ex);
        } finally {
            try {
                executorService.shutdownNow();
                executorService.awaitTermination(1, TimeUnit.MINUTES);
            } catch (Exception e) {
                LOG.error("Shutdown encounter exception.", e);
            }
        }
    }

    private void logShardProcessingState(Map<String, ShardStatusChecker.ProcessState> shardProcessingState) {
        StringBuilder sb = new StringBuilder();
        sb.append("Shard running status: \n");
        for (Map.Entry<String, ShardStatusChecker.ProcessState> entry : shardProcessingState.entrySet()) {
            sb.append("ShardId:").append(entry.getKey()).
                    append(", ProcessingState: ").append(entry.getValue()).append("\n");
        }
        LOG.info("Version: {}, Reader status: {}", streamJob.getVersion(), sb.toString());
    }

    private void checkProcessorRunningStatus(List<RecordProcessor> processors) {
        long now = System.currentTimeMillis();
        for (RecordProcessor processor : processors) {
            RecordProcessor.State state = processor.getState();
            StreamShard shard = processor.getShard();
            if (state == RecordProcessor.State.READY || state == RecordProcessor.State.SUCCEED) {
                continue;
            } else if (state == RecordProcessor.State.INTERRUPTED || state == RecordProcessor.State.FAILED) {
                throw new OTSStreamReaderException("Read task for shard '" + shard.getShardId() + "' has failed.");
            } else { // status = RUNNING
                long lastProcessTime = processor.getLastProcessTime();
                if (now - lastProcessTime > OTSStreamReaderConstants.MAX_ONCE_PROCESS_TIME_MILLIS) {
                    throw new OTSStreamReaderException("Process shard timeout, ShardId:" + shard.getShardId() + ", LastProcessTime:"
                            + lastProcessTime + ", MaxProcessTime:" + OTSStreamReaderConstants.MAX_ONCE_PROCESS_TIME_MILLIS + ", Now:" + now + ".");
                }
            }
        }
    }

    void checkCheckpoint(Map<String, ShardCheckpoint> checkpointMap, StreamJob streamJob) {
        for (Map.Entry<String, ShardCheckpoint> entry : checkpointMap.entrySet()) {
            String shardId = entry.getKey();
            String version = entry.getValue().getVersion();
            if (!streamJob.getShardIds().contains(shardId)) {
                LOG.info("Shard '{}' is not found in job. Job: {}.", entry.getKey(), streamJob.getShardIds());
                throw DataXException.asDataXException(OTSReaderError.ERROR, "Some shard from checkpoint is not belong to this job: " + shardId);
            }

            if (!version.equals(streamJob.getVersion())) {
                LOG.info("Version of shard '{}' in checkpoint is not equal with version of this job. " +
                        "Checkpoint version: {}, job version: {}.", shardId, version, streamJob.getVersion());
                throw DataXException.asDataXException(OTSReaderError.ERROR, "Version of checkpoint is not equal with version of this job.");
            }
        }
    }

    public void close() {
        ots.shutdown();
    }
}
