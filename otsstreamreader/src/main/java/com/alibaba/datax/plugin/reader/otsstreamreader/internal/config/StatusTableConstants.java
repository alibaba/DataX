package com.alibaba.datax.plugin.reader.otsstreamreader.internal.config;

import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;

import java.util.Arrays;
import java.util.List;

public class StatusTableConstants {
    // status table's schema
    public static String PK1_STREAM_ID = "StreamId";
    public static String PK2_STATUS_TYPE = "StatusType";
    public static String PK3_STATUS_VALUE = "StatusValue";

    public static List<PrimaryKeySchema> STATUS_TABLE_PK_SCHEMA = Arrays.asList(
            new PrimaryKeySchema(PK1_STREAM_ID, PrimaryKeyType.STRING),
            new PrimaryKeySchema(PK2_STATUS_TYPE, PrimaryKeyType.STRING),
            new PrimaryKeySchema(PK3_STATUS_VALUE, PrimaryKeyType.STRING));

    /**
     * 记录对应某一时刻的所有Shard的Checkpoint。
     * 格式如下：
     *
     * PK1 : StreamId   : "dataTable_131231"
     * PK2 : StatusType  : "CheckpointForDataxReader"
     *
     * 记录Checkpoint：
     *      PK3    : StatusValue : "1444357620415   shard1"  (Time + \t + ShardId)
     *      Column : Checkpoint  : "checkpoint"
     * 记录ShardCount：
     *      PK3    : StatusValue : "1444357620415"   (Time)
     *      Column : ShardCount  : 3
     *
     */
    public static String STATUS_TYPE_CHECKPOINT = "CheckpointForDataxReader";

    // 记录每次Datax Job的运行信息，包括Shard列表，StreamId和版本等。
    public static String STATUS_TYPE_JOB_DESC = "DataxJobDesc";

    /**
     * 记录某个Shard在某个时间的Checkpoint
     * PK1: StreamId : "dataTable_131231"
     * PK2: StatusType: "ShardTimeCheckpointForDataxReader"
     * PK3: StatusValue: "shard1    1444357620415"  (ShardId + \t + Time)
     * Column: Checkpoint : "checkpoint"
     */
    public static String STATUS_TYPE_SHARD_CHECKPOINT = "ShardTimeCheckpointForDataxReader";

    public static String TIME_SHARD_SEPARATOR = "\t";
    public static String LARGEST_SHARD_ID = String.valueOf((char)127);  //用于确定GetRange的范围。

    // 记录Checkpoint的行的属性列
    public static String CHECKPOINT_COLUMN_NAME = "Checkpoint";
    public static String VERSION_COLUMN_NAME = "Version";
    public static String SKIP_COUNT_COLUMN_NAME = "SkipCount";
    public static String SHARDCOUNT_COLUMN_NAME = "ShardCount";

    // 记录Job信息的行的属性列
    public static final int COLUMN_MAX_SIZE = 64 * 1024;
    public static final String JOB_SHARD_LIST_PREFIX_COLUMN_NAME = "ShardIds_";
    public static final String JOB_VERSION_COLUMN_NAME = "Version";
    public static final String JOB_TABLE_NAME_COLUMN_NAME = "TableName";
    public static final String JOB_STREAM_ID_COLUMN_NAME = "JobStreamId";
    public static final String JOB_START_TIME_COLUMN_NAME = "StartTime";
    public static final String JOB_END_TIME_COLUMN_NAME = "EndTime";

}
