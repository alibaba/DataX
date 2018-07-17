package com.alibaba.datax.plugin.reader.otsstreamreader.internal.model;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.OTSReaderError;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.StatusTableConstants;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.GsonParser;
import com.alicloud.openservices.tablestore.core.utils.CompressUtil;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.google.gson.Gson;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class StreamJob {
    private String tableName;
    private String streamId;
    private String version;
    private Set<String> shardIds;
    private long startTimeInMillis;
    private long endTimeInMillis;

    public StreamJob(String tableName, String streamId, String version,
                     Set<String> shardIds, long startTimestampMillis, long endTimestampMillis) {
        this.tableName = tableName;
        this.streamId = streamId;
        this.version = version;
        this.shardIds = shardIds;
        this.startTimeInMillis = startTimestampMillis;
        this.endTimeInMillis = endTimestampMillis;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Set<String> getShardIds() {
        return shardIds;
    }

    public void setShardIds(Set<String> shardIds) {
        this.shardIds = shardIds;
    }

    public long getStartTimeInMillis() {
        return startTimeInMillis;
    }

    public void setStartTimeInMillis(long startTimeInMillis) {
        this.startTimeInMillis = startTimeInMillis;
    }

    public long getEndTimeInMillis() {
        return endTimeInMillis;
    }

    public void setEndTimeInMillis(long endTimeInMillis) {
        this.endTimeInMillis = endTimeInMillis;
    }

    public void serializeShardIdList(RowPutChange rowChange, Set<String> shardIds) {
        try {
            String json = GsonParser.listToJson(new ArrayList<String>(shardIds));
            byte[] content = CompressUtil.compress(new ByteArrayInputStream(json.getBytes("utf-8")), new Deflater());
            List<ColumnValue> columns = new ArrayList<ColumnValue>();
            int index = 0;
            while (index < content.length) {
                int endIndex = index + StatusTableConstants.COLUMN_MAX_SIZE;
                if (endIndex > content.length) {
                    endIndex = content.length;
                }

                columns.add(ColumnValue.fromBinary(Arrays.copyOfRange(content, index, endIndex)));

                index = endIndex;
            }

            for (int id = 0; id < columns.size(); id++) {
                rowChange.addColumn(StatusTableConstants.JOB_SHARD_LIST_PREFIX_COLUMN_NAME + id, columns.get(id));
            }
        } catch (UnsupportedEncodingException e) {
            throw DataXException.asDataXException(OTSReaderError.ERROR, e);
        } catch (IOException e) {
            throw DataXException.asDataXException(OTSReaderError.ERROR, e);
        }
    }

    public static Set<String> deserializeShardIdList(Row row) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        try {
            int id = 0;
            while (true) {
                String columnName = StatusTableConstants.JOB_SHARD_LIST_PREFIX_COLUMN_NAME + id;
                Column column = row.getLatestColumn(columnName);
                if (column != null) {
                    output.write(column.getValue().asBinary());
                    id++;
                } else {
                    break;
                }
            }

            byte[] content = output.toByteArray();

            byte[] realContent = CompressUtil.decompress(new ByteArrayInputStream(content), 1024, new Inflater());
            String json = new String(realContent, "utf-8");
            return new HashSet<String>(GsonParser.jsonToList(json));
        } catch (UnsupportedEncodingException e) {
            throw DataXException.asDataXException(OTSReaderError.ERROR, e);
        } catch (IOException e) {
            throw DataXException.asDataXException(OTSReaderError.ERROR, e);
        } catch (DataFormatException e) {
            throw DataXException.asDataXException(OTSReaderError.ERROR, e);
        }
    }

    public void serializeColumn(RowPutChange rowChange) {
        serializeShardIdList(rowChange, shardIds);
        rowChange.addColumn(StatusTableConstants.JOB_VERSION_COLUMN_NAME, ColumnValue.fromString(version));
        rowChange.addColumn(StatusTableConstants.JOB_TABLE_NAME_COLUMN_NAME, ColumnValue.fromString(tableName));
        rowChange.addColumn(StatusTableConstants.JOB_STREAM_ID_COLUMN_NAME, ColumnValue.fromString(streamId));
        rowChange.addColumn(StatusTableConstants.JOB_START_TIME_COLUMN_NAME, ColumnValue.fromLong(startTimeInMillis));
        rowChange.addColumn(StatusTableConstants.JOB_END_TIME_COLUMN_NAME, ColumnValue.fromLong(endTimeInMillis));
    }

    public String toJson() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    @Override
    public String toString() {
        return toJson();
    }

    public static StreamJob fromJson(String json) {
        Gson gson = new Gson();
        return gson.fromJson(json, StreamJob.class);
    }

    public static StreamJob fromRow(Row row) {
        if (row == null) {
            return null;
        }

        Set<String> shardIds = deserializeShardIdList(row);
        String version = row.getLatestColumn(StatusTableConstants.JOB_VERSION_COLUMN_NAME).getValue().asString();
        String tableName = row.getLatestColumn(StatusTableConstants.JOB_TABLE_NAME_COLUMN_NAME).getValue().asString();
        String streamId = row.getLatestColumn(StatusTableConstants.JOB_STREAM_ID_COLUMN_NAME).getValue().asString();
        long startTime = row.getLatestColumn(StatusTableConstants.JOB_START_TIME_COLUMN_NAME).getValue().asLong();
        long endTime = row.getLatestColumn(StatusTableConstants.JOB_END_TIME_COLUMN_NAME).getValue().asLong();

        return new StreamJob(tableName, streamId, version, shardIds, startTime, endTime);
    }
}
