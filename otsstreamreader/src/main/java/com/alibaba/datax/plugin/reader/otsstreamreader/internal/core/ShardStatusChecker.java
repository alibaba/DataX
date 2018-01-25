package com.alibaba.datax.plugin.reader.otsstreamreader.internal.core;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.OTSReaderError;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.ShardCheckpoint;
import com.alicloud.openservices.tablestore.model.StreamShard;
import com.aliyun.openservices.ots.internal.streamclient.model.CheckpointPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ShardStatusChecker {

    private static final Logger LOG = LoggerFactory.getLogger(ShardStatusChecker.class);

    public enum ProcessState {
        READY,           // shard is ready to process records and is not done
        DONE_NOT_END,    // shard is done but not reach end of shard
        DONE_REACH_END,  // shard is done and reach end of shard
        BLOCK,           // shard is block on its parents
        SKIP             // shard is skipped
    }

    /**
     * 1. 若shard没有parent shard，或者其parent shard均已达到END，则该shard需要被处理
     * 2. 若shard有parent shard，其已经被处理完毕，且其checkpoint不为END，则该shard不需要再被处理
     * <p/>
     * 所有确认需要被处理和不需要被处理的shard，都会从allShardToProcess列表中移除
     *
     * @param allShardToProcess
     * @param allShardsMap
     * @param checkpointMap
     * @return
     */
    public static void findShardToProcess(
            Map<String, StreamShard> allShardToProcess,
            Map<String, StreamShard> allShardsMap,
            Map<String, ShardCheckpoint> checkpointMap,
            List<StreamShard> shardToProcess,
            List<StreamShard> shardNoNeedToProcess,
            List<StreamShard> shardBlocked) {
        Map<String, ProcessState> shardStates = new HashMap<String, ProcessState>();
        for (Map.Entry<String, StreamShard> entry : allShardToProcess.entrySet()) {
            determineShardState(entry.getValue().getShardId(), allShardsMap, checkpointMap, shardStates);
        }

        for (Map.Entry<String, ProcessState> entry : shardStates.entrySet()) {
            String shardId = entry.getKey();
            if (allShardToProcess.containsKey(shardId)) {
                StreamShard shard = allShardToProcess.get(shardId);
                switch (entry.getValue()) {
                    case READY:
                        shardToProcess.add(shard);
                        allShardToProcess.remove(shardId);
                        break;
                    case BLOCK:
                        shardBlocked.add(shard);
                        break;
                    case SKIP:
                        shardNoNeedToProcess.add(shard);
                        allShardToProcess.remove(shardId);
                        break;
                    default:
                        LOG.error("Unexpected state '{}' for shard '{}'.", entry.getValue(), shard);
                        throw DataXException.asDataXException(OTSReaderError.ERROR, "Unexpected state '" + entry.getValue() + "' for shard '" + shard + "'.");
                }
            }
        }
    }

    public static ProcessState determineShardState(
            String shardId,
            Map<String, StreamShard> allShards,
            Map<String, ShardCheckpoint> allCheckpoints,
            Map<String, ProcessState> shardStates) {
        StreamShard shard = allShards.get(shardId);
        if (shard == null) {
            // 若发现shard已经不存在，则我们认为shard已经被处理完毕。
            // 做出这种判断的前提是：
            //      若此次任务是延续上次任务的checkpoint，则该shard一定是在上一次任务中checkpoint达到了SHARD_END(在slave初始化时做检查)。
            //      若此次任务不是延续上次任务，则对于全新的任务，不存在的shard我们可以认为是处理完毕的，即不需要处理。
            LOG.warn("Shard is not found: {}.", shardId);
            return ProcessState.DONE_REACH_END;
        }

        if (shardStates.containsKey(shardId)) {
            return shardStates.get(shardId);
        }

        ProcessState finalState;

        if (allCheckpoints.containsKey(shardId)) {
            ShardCheckpoint checkpoint = allCheckpoints.get(shardId);
            if (checkpoint == null || checkpoint.getCheckpoint() == null) {
                finalState = ProcessState.READY;
            } else if (checkpoint.getCheckpoint().equals(CheckpointPosition.SHARD_END)){
                finalState = ProcessState.DONE_REACH_END;
            } else {
                finalState = ProcessState.DONE_NOT_END;
            }
        } else {
            ProcessState stateOfParent = ProcessState.DONE_REACH_END;
            String parentId = shard.getParentId();
            if (parentId != null) {
                stateOfParent = determineShardState(parentId, allShards, allCheckpoints, shardStates);
            }

            ProcessState stateOfParentSibling = ProcessState.DONE_REACH_END;
            String parentSiblingId = shard.getParentSiblingId();
            if (parentSiblingId != null) {
                stateOfParentSibling = determineShardState(parentSiblingId, allShards, allCheckpoints, shardStates);
            }

            if (stateOfParent == ProcessState.SKIP || stateOfParentSibling == ProcessState.SKIP) {
                finalState = ProcessState.SKIP;
            } else if (stateOfParent == ProcessState.DONE_NOT_END || stateOfParentSibling == ProcessState.DONE_NOT_END) {
                finalState = ProcessState.SKIP;
            } else if (stateOfParent == ProcessState.BLOCK || stateOfParentSibling == ProcessState.BLOCK) {
                finalState = ProcessState.BLOCK;
            } else if (stateOfParent == ProcessState.READY || stateOfParentSibling == ProcessState.READY){
                finalState = ProcessState.BLOCK;
            } else { // stateOfParent == ProcessState.DONE_REACH_END && stateOfParentSibling == ProcessState.DONE_REACH_END
                finalState = ProcessState.READY;
            }
        }

        shardStates.put(shard.getShardId(), finalState);
        return finalState;
    }
}
