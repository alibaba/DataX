package com.alibaba.datax.plugin.reader.otsstreamreader.internal.core;

import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.StatusTableConstants;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.ShardCheckpoint;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.StreamJob;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.GsonParser;
import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class CheckpointTimeTracker {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointTimeTracker.class);

    private final SyncClientInterface client;
    private final String statusTable;
    private final String streamId;

    public CheckpointTimeTracker(SyncClientInterface client, String statusTable, String streamId) {
        this.client = client;
        this.statusTable = statusTable;
        this.streamId = streamId;
    }

    /**
     * 返回timestamp时刻记录了checkpoint的shard的个数，用于检查checkpoints是否完整。
     *
     * @param timestamp
     * @return 如果status表中未记录shardCount信息，返回－1
     */
    public int getShardCountForCheck(long timestamp) {
        PrimaryKey primaryKey = getPrimaryKeyForShardCount(timestamp);
        GetRowRequest getRowRequest = getOTSRequestForGet(primaryKey);
        Row row = client.getRow(getRowRequest).getRow();
        if (row == null) {
            return -1;
        }
        int shardCount = (int) row.getColumn(StatusTableConstants.SHARDCOUNT_COLUMN_NAME).get(0).getValue().asLong();
        LOG.info("GetShardCount: timestamp: {}, shardCount: {}.", timestamp, shardCount);
        return shardCount;
    }


    /**
     * 从状态表中读取所有的checkpoint。
     *
     * @param timestamp
     * @return
     */
    public Map<String, ShardCheckpoint> getAllCheckpoints(long timestamp) {
        Iterator<Row> rowIter = getRangeIteratorForGetAllCheckpoints(client, timestamp);
        List<Row> rows = readAllRows(rowIter);

        Map<String, ShardCheckpoint> checkpointMap = new HashMap<String, ShardCheckpoint>();
        for (Row row : rows) {
            String pk3 = row.getPrimaryKey().getPrimaryKeyColumn(StatusTableConstants.PK3_STATUS_VALUE).getValue().asString();
            String shardId = pk3.split(StatusTableConstants.TIME_SHARD_SEPARATOR)[1];

            ShardCheckpoint checkpoint = ShardCheckpoint.fromRow(shardId, row);
            checkpointMap.put(shardId, checkpoint);
        }

        if (LOG.isDebugEnabled()) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("GetAllCheckpoints: size: " + checkpointMap.size());
            for (String shardId : checkpointMap.keySet()) {
                stringBuilder.append(", [shardId: ");
                stringBuilder.append(shardId);
                stringBuilder.append(", checkpoint: ");
                stringBuilder.append(checkpointMap.get(shardId));
                stringBuilder.append("]");
            }
            LOG.debug(stringBuilder.toString());
        }
        return checkpointMap;
    }

    private List<Row> readAllRows(Iterator<Row> rowIter) {
        List<Row> rows = new ArrayList<Row>();
        while (rowIter.hasNext()) {
            rows.add(rowIter.next());
        }
        return rows;
    }

    /**
     * 设置某个分片某个时间的checkpoint, 用于寻找某个分片在一定区间内较大的checkpoint, 减少扫描的数据量.
     *
     * @param shardId
     * @param timestamp
     * @param checkpointValue
     */
    public void setShardTimeCheckpoint(String shardId, long timestamp, String checkpointValue) {
        PutRowRequest putRowRequest = getOTSRequestForSetShardTimeCheckpoint(shardId, timestamp, checkpointValue);
        client.putRow(putRowRequest);
        LOG.info("SetShardTimeCheckpoint: timestamp: {}, shardId: {}, checkpointValue: {}.", timestamp, shardId, checkpointValue);
    }

    /**
     * 获取某个分片在某个时间范围内最大的checkpoint, 用于寻找某个分片在一定区间内较大的checkpoint, 减少扫描的数据量.
     * 查询的范围为左开右闭。
     *
     * @param shardId
     * @param startTimestamp
     * @param endTimestamp
     * @return
     */
    public String getShardLargestCheckpointInTimeRange(String shardId, long startTimestamp, long endTimestamp) {
        PrimaryKey startPk = getPrimaryKeyForShardTimeCheckpoint(shardId, endTimestamp);
        PrimaryKey endPk = getPrimaryKeyForShardTimeCheckpoint(shardId, startTimestamp);
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(statusTable);
        rangeRowQueryCriteria.setMaxVersions(1);
        rangeRowQueryCriteria.setDirection(Direction.BACKWARD);
        rangeRowQueryCriteria.setLimit(1);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(startPk);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPk);
        GetRangeRequest getRangeRequest = new GetRangeRequest(rangeRowQueryCriteria);

        GetRangeResponse result = client.getRange(getRangeRequest);
        if (result.getRows().isEmpty()) {
            return null;
        } else {
            try {
                String checkpoint = result.getRows().get(0).getLatestColumn(StatusTableConstants.CHECKPOINT_COLUMN_NAME).getValue().asString();
                String time = result.getRows().get(0).getPrimaryKey().getPrimaryKeyColumn(2).getValue().asString().split(StatusTableConstants.TIME_SHARD_SEPARATOR)[1];
                LOG.info("find checkpoint for shard {} in time {}.", shardId, time);
                return checkpoint;
            } catch (Exception ex) {
                LOG.error("Error when get shard time checkpoint.", ex);
                return null;
            }
        }
    }

    public void clearAllCheckpoints(long timestamp) {
        Iterator<Row> rowIter = getRangeIteratorForGetAllCheckpoints(client, timestamp);
        List<Row> rows = readAllRows(rowIter);

        for (Row row : rows) {
            DeleteRowRequest deleteRowRequest = getOTSRequestForDelete(row.getPrimaryKey());
            client.deleteRow(deleteRowRequest);
        }

        LOG.info("ClearAllCheckpoints: timestamp: {}.", timestamp);
    }

    private PrimaryKey getPrimaryKeyForCheckpoint(long timestamp, String shardId) {
        String statusValue = String.format("%16d", timestamp) + StatusTableConstants.TIME_SHARD_SEPARATOR + shardId;

        List<PrimaryKeyColumn> pkCols = new ArrayList<PrimaryKeyColumn>();
        pkCols.add(new PrimaryKeyColumn(StatusTableConstants.PK1_STREAM_ID, PrimaryKeyValue.fromString(streamId)));
        pkCols.add(new PrimaryKeyColumn(StatusTableConstants.PK2_STATUS_TYPE, PrimaryKeyValue.fromString(StatusTableConstants.STATUS_TYPE_CHECKPOINT)));
        pkCols.add(new PrimaryKeyColumn(StatusTableConstants.PK3_STATUS_VALUE, PrimaryKeyValue.fromString(statusValue)));

        PrimaryKey primaryKey = new PrimaryKey(pkCols);
        return primaryKey;
    }

    private PrimaryKey getPrimaryKeyForJobDesc(long timestamp) {
        String statusValue = String.format("%16d", timestamp);

        List<PrimaryKeyColumn> pkCols = new ArrayList<PrimaryKeyColumn>();
        pkCols.add(new PrimaryKeyColumn(StatusTableConstants.PK1_STREAM_ID, PrimaryKeyValue.fromString(streamId)));
        pkCols.add(new PrimaryKeyColumn(StatusTableConstants.PK2_STATUS_TYPE, PrimaryKeyValue.fromString(StatusTableConstants.STATUS_TYPE_JOB_DESC)));
        pkCols.add(new PrimaryKeyColumn(StatusTableConstants.PK3_STATUS_VALUE, PrimaryKeyValue.fromString(statusValue)));

        PrimaryKey primaryKey = new PrimaryKey(pkCols);
        return primaryKey;
    }

    public PrimaryKey getPrimaryKeyForShardCount(long timestamp) {
        String statusValue = String.format("%16d", timestamp);

        List<PrimaryKeyColumn> pkCols = new ArrayList<PrimaryKeyColumn>();
        pkCols.add(new PrimaryKeyColumn(StatusTableConstants.PK1_STREAM_ID, PrimaryKeyValue.fromString(streamId)));
        pkCols.add(new PrimaryKeyColumn(StatusTableConstants.PK2_STATUS_TYPE, PrimaryKeyValue.fromString(StatusTableConstants.STATUS_TYPE_CHECKPOINT)));
        pkCols.add(new PrimaryKeyColumn(StatusTableConstants.PK3_STATUS_VALUE, PrimaryKeyValue.fromString(statusValue)));

        PrimaryKey primaryKey = new PrimaryKey(pkCols);
        return primaryKey;
    }

    private PrimaryKey getPrimaryKeyForShardTimeCheckpoint(String shardId, long timestamp) {
        String statusValue = shardId + StatusTableConstants.TIME_SHARD_SEPARATOR + String.format("%16d", timestamp);

        List<PrimaryKeyColumn> pkCols = new ArrayList<PrimaryKeyColumn>();
        pkCols.add(new PrimaryKeyColumn(StatusTableConstants.PK1_STREAM_ID, PrimaryKeyValue.fromString(streamId)));
        pkCols.add(new PrimaryKeyColumn(StatusTableConstants.PK2_STATUS_TYPE, PrimaryKeyValue.fromString(StatusTableConstants.STATUS_TYPE_SHARD_CHECKPOINT)));
        pkCols.add(new PrimaryKeyColumn(StatusTableConstants.PK3_STATUS_VALUE, PrimaryKeyValue.fromString(statusValue)));

        PrimaryKey primaryKey = new PrimaryKey(pkCols);
        return primaryKey;
    }

    private PutRowRequest getOTSRequestForSetShardTimeCheckpoint(String shardId, long timestamp, String checkpointValue) {
        PrimaryKey primaryKey = getPrimaryKeyForShardTimeCheckpoint(shardId, timestamp);

        RowPutChange rowPutChange = new RowPutChange(statusTable, primaryKey);
        rowPutChange.addColumn(StatusTableConstants.CHECKPOINT_COLUMN_NAME, ColumnValue.fromString(checkpointValue));

        PutRowRequest putRowRequest = new PutRowRequest(rowPutChange);
        return putRowRequest;
    }

    private GetRowRequest getOTSRequestForGet(PrimaryKey primaryKey) {
        SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(statusTable, primaryKey);
        rowQueryCriteria.setMaxVersions(1);

        GetRowRequest getRowRequest = new GetRowRequest(rowQueryCriteria);
        return getRowRequest;
    }

    private Iterator<Row> getRangeIteratorForGetAllCheckpoints(SyncClientInterface client, long timestamp) {
        RangeIteratorParameter param = new RangeIteratorParameter(statusTable);

        PrimaryKey startPk = getPrimaryKeyForCheckpoint(timestamp, "");
        PrimaryKey endPk = getPrimaryKeyForCheckpoint(timestamp, StatusTableConstants.LARGEST_SHARD_ID);
        param.setMaxVersions(1);
        param.setInclusiveStartPrimaryKey(startPk);
        param.setExclusiveEndPrimaryKey(endPk);

        return client.createRangeIterator(param);
    }

    private DeleteRowRequest getOTSRequestForDelete(PrimaryKey primaryKey) {
        RowDeleteChange rowDeleteChange = new RowDeleteChange(statusTable, primaryKey);
        DeleteRowRequest deleteRowRequest = new DeleteRowRequest(rowDeleteChange);
        return deleteRowRequest;
    }

    public void writeCheckpoint(long timestamp, ShardCheckpoint checkpoint) {
        writeCheckpoint(timestamp, checkpoint, 0);
    }

    public void writeCheckpoint(long timestamp, ShardCheckpoint checkpoint, long sendRecordCount) {
        LOG.info("Write checkpoint of time '{}' of shard '{}'.", timestamp, checkpoint.getShardId());
        PrimaryKey primaryKey = getPrimaryKeyForCheckpoint(timestamp, checkpoint.getShardId());

        RowPutChange rowChange = new RowPutChange(statusTable, primaryKey);
        checkpoint.serializeColumn(rowChange);

        if (sendRecordCount > 0) {
            rowChange.addColumn("SendRecordCount", ColumnValue.fromLong(sendRecordCount));
        }

        PutRowRequest request = new PutRowRequest();
        request.setRowChange(rowChange);
        client.putRow(request);
    }

    public ShardCheckpoint readCheckpoint(String shardId, long timestamp) {
        PrimaryKey primaryKey = getPrimaryKeyForCheckpoint(timestamp, shardId);
        GetRowRequest getRowRequest = getOTSRequestForGet(primaryKey);
        Row row = client.getRow(getRowRequest).getRow();
        if (row == null) {
            return null;
        }

        return ShardCheckpoint.fromRow(shardId, row);
    }

    public void writeStreamJob(StreamJob streamJob) {
        PrimaryKey primaryKey = getPrimaryKeyForJobDesc(streamJob.getEndTimeInMillis());

        RowPutChange rowChange = new RowPutChange(statusTable);
        rowChange.setPrimaryKey(primaryKey);
        streamJob.serializeColumn(rowChange);

        PutRowRequest request = new PutRowRequest();
        request.setRowChange(rowChange);
        client.putRow(request);
    }

    public StreamJob readStreamJob(long timestamp) {
        PrimaryKey primaryKey = getPrimaryKeyForJobDesc(timestamp);
        GetRowRequest request = getOTSRequestForGet(primaryKey);

        GetRowResponse response = client.getRow(request);
        return StreamJob.fromRow(response.getRow());
    }

    /**
     * 获取指定timestamp对应的Job的checkpoint，并检查checkpoint是否完整。
     * 若是老版本的Job，则只检查shardCount是否一致。
     * 若是新版本的Job，则除了检查shard id列表完全一致，还需要检查每个shard的checkpoint的version是否与job描述内的一致。
     *
     * @param timestamp
     * @param streamId
     * @param allCheckpoints
     * @return 若成功获取上一次Job完整的checkpoint，则返回true，否则返回false
     */
    public boolean getAndCheckAllCheckpoints(long timestamp, String streamId, Map<String, ShardCheckpoint> allCheckpoints) {
        allCheckpoints.clear();
        Map<String, ShardCheckpoint> allCheckpointsInTable = getAllCheckpoints(timestamp);

        long shardCount = -1;
        boolean checkShardCountOnly = false;
        StreamJob streamJob = readStreamJob(timestamp);
        if (streamJob == null) {
            LOG.info("Stream job is not exist, timestamp: {}.", timestamp);

            // 如果streamJob不存在，则有可能是老版本的Job，尝试读取shardCount
            shardCount = getShardCountForCheck(timestamp);
            if (shardCount == -1) {
                LOG.info("Shard count not found, timestamp: {}.", timestamp);
                return false;
            }

            checkShardCountOnly = true;
        }

        if (checkShardCountOnly) {
            if (shardCount != allCheckpointsInTable.size()) {
                LOG.info("Shard count not equal, shardCount: {}, checkpointCount: {}.", shardCount, allCheckpoints.size());
                return false;
            }
        } else {
            // 检查streamJob内的信息是否与checkpoint一致
            if (!streamJob.getStreamId().equals(streamId)) {
                LOG.info("Stream id of the checkpoint is not equal with current job. StreamIdInCheckpoint: {}, StreamId: {}.",
                        streamJob.getStreamId(), streamId);
                return false;
            }

            if (streamJob.getShardIds().size() != allCheckpointsInTable.size()) {
                LOG.info(
                        "Shards in stream job is not equal with checkpoint count. " +
                                "StreamJob shard count: {}, checkpoint count: {}.",
                        streamJob.getShardIds().size(), allCheckpointsInTable.size());
                return false;
            }

            for (String shardId : streamJob.getShardIds()) {
                ShardCheckpoint checkpoint = allCheckpointsInTable.get(shardId);
                if (checkpoint == null) {
                    LOG.info("Checkpoint of shard in job is not found. ShardId: {}.", shardId);
                    return false;
                }

                if (!checkpoint.getVersion().equals(streamJob.getVersion())) {
                    LOG.info("Version is different. Checkpoint: {}, StreamJob: {}.", checkpoint, streamJob);
                    return false;
                }
            }
        }

        for (Map.Entry<String, ShardCheckpoint> entry : allCheckpointsInTable.entrySet()) {
            allCheckpoints.put(entry.getKey(), entry.getValue());
        }
        return true;
    }
}
