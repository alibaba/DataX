package com.alibaba.datax.plugin.reader.otsstreamreader.internal.core;

import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConstants;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.OTSStreamReaderException;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.StatusTableConstants;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.ShardCheckpoint;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.StreamJob;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.OTSHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.TimeUtils;
import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.openservices.ots.internal.streamclient.Worker;
import com.aliyun.openservices.ots.internal.streamclient.model.CheckpointPosition;
import com.aliyun.openservices.ots.internal.streamclient.model.WorkerStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OTSStreamReaderChecker {
    private static final Logger LOG = LoggerFactory.getLogger(OTSStreamReaderChecker.class);


    private final SyncClientInterface ots;
    private final OTSStreamReaderConfig config;

    public OTSStreamReaderChecker(SyncClientInterface ots, OTSStreamReaderConfig config) {
        this.ots = ots;
        this.config = config;
    }

    /**
     * 1. 检查dataTable是否开启了stream。
     * 2. 检查要导出的时间范围是否合理：
     *      最大可导出的时间范围为： ［now － expirationTime, now]
     *      为了避免时间误差影响，允许导出的范围为： [now - expirationTime + beforeOffset, now - afterOffset]
     */
    public void checkStreamEnabledAndTimeRangeOK() {
        boolean exists = OTSHelper.checkTableExists(ots, config.getDataTable(), config.isTimeseriesTable());
        if (!exists) {
            throw new OTSStreamReaderException("The data table is not exist.");
        }
        StreamDetails streamDetails = OTSHelper.getStreamDetails(ots, config.getDataTable(), config.isTimeseriesTable());
        if (streamDetails == null || !streamDetails.isEnableStream()) {
            throw new OTSStreamReaderException("The stream of data table is not enabled.");
        }
        long now = System.currentTimeMillis();
        long startTime = config.getStartTimestampMillis();
        long endTime = config.getEndTimestampMillis();
        long beforeOffset = OTSStreamReaderConstants.BEFORE_OFFSET_TIME_MILLIS;
        long afterOffset = OTSStreamReaderConstants.AFTER_OFFSET_TIME_MILLIS;
        long expirationTime = streamDetails.getExpirationTime() * TimeUtils.HOUR_IN_MILLIS;

        if (startTime < now - expirationTime + beforeOffset) {
            throw new OTSStreamReaderException("As expiration time is " + expirationTime + ", so the start timestamp must greater than "
                    + TimeUtils.getTimeInISO8601(new Date(now - expirationTime + beforeOffset)) + "(" + (now - expirationTime + beforeOffset )+ ")");
        }

        if (endTime > now - afterOffset) {
            throw new OTSStreamReaderException("To avoid timing error between different machines, the end timestamp must smaller" +
                    " than " + TimeUtils.getTimeInISO8601(new Date(now - afterOffset)) + "(" + (now - afterOffset) + ")");
        }
    }

    /**
     * 检查statusTable的tableMeta
     * @param tableMeta
     */
    private void checkTableMetaOfStatusTable(TableMeta tableMeta) {
        List<PrimaryKeySchema> pkSchema = tableMeta.getPrimaryKeyList();
        if (!pkSchema.equals(StatusTableConstants.STATUS_TABLE_PK_SCHEMA)) {
            throw new OTSStreamReaderException("Unexpected table meta in status table, please check your config.");
        }
    }

    /**
     * 检查statusTable是否存在，如果不存在就创建statusTable，并等待表ready。
     */
    public void checkAndCreateStatusTableIfNotExist() {
        boolean tableExist = OTSHelper.checkTableExists(ots, config.getStatusTable(), false);
        if (tableExist) {
            DescribeTableResponse describeTableResult = OTSHelper.describeTable(ots, config.getStatusTable());
            checkTableMetaOfStatusTable(describeTableResult.getTableMeta());
        } else {
            TableMeta tableMeta = new TableMeta(config.getStatusTable());
            tableMeta.addPrimaryKeyColumns(StatusTableConstants.STATUS_TABLE_PK_SCHEMA);
            TableOptions tableOptions = new TableOptions(OTSStreamReaderConstants.STATUS_TABLE_TTL, 1);
            OTSHelper.createTable(ots, tableMeta, tableOptions);
            boolean tableReady = OTSHelper.waitUntilTableReady(ots, config.getStatusTable(),
                    OTSStreamReaderConstants.MAX_WAIT_TABLE_READY_TIME_MILLIS);
            if (!tableReady) {
                throw new OTSStreamReaderException("Check table ready timeout, MaxWaitTableReadyTimeMillis:"
                    + OTSStreamReaderConstants.MAX_WAIT_TABLE_READY_TIME_MILLIS + ".");
            }
        }
    }

    /**
     * 尝试从状态表中恢复上一次Job执行结束后的checkpoint。
     * 若恢复成功，则返回true，否则返回false。
     *
     * @param checkpointTimeTracker
     * @param allShardsMap
     *@param streamJob
     * @param currentShardCheckpointMap   @return
     */
    public boolean checkAndSetCheckpoints(
            CheckpointTimeTracker checkpointTimeTracker,
            Map<String, StreamShard> allShardsMap,
            StreamJob streamJob,
            Map<String, ShardCheckpoint> currentShardCheckpointMap) {
        long timestamp = config.getStartTimestampMillis();
        Map<String, ShardCheckpoint> allCheckpoints = new HashMap<String, ShardCheckpoint>();
        boolean gotCheckpoint = checkpointTimeTracker.getAndCheckAllCheckpoints(timestamp, streamJob.getStreamId(), allCheckpoints);
        if (!gotCheckpoint) {
            return false;
        }

        for (Map.Entry<String, ShardCheckpoint> entry : allCheckpoints.entrySet()) {
            String shardId = entry.getKey();
            ShardCheckpoint checkpoint = entry.getValue();
            if (!currentShardCheckpointMap.containsKey(shardId)) {
                // 发现未读完的shard，并且该shard还不在此次任务列表中
                if (!checkpoint.getCheckpoint().equals(CheckpointPosition.SHARD_END)) {
                    throw new OTSStreamReaderException("Shard does not exist now, ShardId:"
                        + shardId + ", Checkpoint:" + checkpoint);
                }
            } else {
                currentShardCheckpointMap.put(shardId, new ShardCheckpoint(shardId, streamJob.getVersion(),
                        checkpoint.getCheckpoint(), checkpoint.getSkipCount()));
            }
        }

        return true;
    }
}
