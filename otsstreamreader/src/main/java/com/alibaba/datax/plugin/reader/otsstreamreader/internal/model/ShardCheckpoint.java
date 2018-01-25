package com.alibaba.datax.plugin.reader.otsstreamreader.internal.model;

import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.StatusTableConstants;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.RowPutChange;

public class ShardCheckpoint {
    private String shardId;
    private String version;
    private String checkpoint;
    private long skipCount;

    public ShardCheckpoint(String shardId, String version, String shardIterator, long skipCount) {
        this.shardId = shardId;
        this.version = version;
        this.checkpoint = shardIterator;
        this.skipCount = skipCount;
    }

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getCheckpoint() {
        return checkpoint;
    }

    public void setCheckpoint(String checkpoint) {
        this.checkpoint = checkpoint;
    }

    public long getSkipCount() {
        return skipCount;
    }

    public void setSkipCount(long skipCount) {
        this.skipCount = skipCount;
    }

    public static ShardCheckpoint fromRow(String shardId, Row row) {
        String shardIterator = row.getLatestColumn(StatusTableConstants.CHECKPOINT_COLUMN_NAME).getValue().asString();

        long skipCount = 0;
        // compatible with old stream reader
        if (row.contains(StatusTableConstants.SKIP_COUNT_COLUMN_NAME)) {
            skipCount = row.getLatestColumn(StatusTableConstants.SKIP_COUNT_COLUMN_NAME).getValue().asLong();
        }

        // compatible with old stream reader
        String version = "";
        if (row.contains(StatusTableConstants.VERSION_COLUMN_NAME)) {
            version = row.getLatestColumn(StatusTableConstants.VERSION_COLUMN_NAME).getValue().asString();
        }

        return new ShardCheckpoint(shardId, version, shardIterator, skipCount);
    }

    public void serializeColumn(RowPutChange rowChange) {
        rowChange.addColumn(StatusTableConstants.VERSION_COLUMN_NAME, ColumnValue.fromString(version));
        rowChange.addColumn(StatusTableConstants.CHECKPOINT_COLUMN_NAME, ColumnValue.fromString(checkpoint));
        rowChange.addColumn(StatusTableConstants.SKIP_COUNT_COLUMN_NAME, ColumnValue.fromLong(skipCount));
    }

    @Override
    public int hashCode() {
        int result = 31;
        result = result ^ this.shardId.hashCode();
        result = result ^ this.version.hashCode();
        result = result ^ this.checkpoint.hashCode();
        result = result ^ (int)this.skipCount;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (!(obj instanceof ShardCheckpoint)) {
            return false;
        }

        ShardCheckpoint other = (ShardCheckpoint)obj;

        return this.shardId.equals(other.shardId) &&
                this.version.equals(other.version) &&
                this.checkpoint.equals(other.checkpoint) &&
                this.skipCount == other.skipCount;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ShardId: ").append(shardId)
                .append(", Version: ").append(version)
                .append(", Checkpoint: ").append(checkpoint)
                .append(", SkipCount: ").append(skipCount);
        return sb.toString();
    }
}
