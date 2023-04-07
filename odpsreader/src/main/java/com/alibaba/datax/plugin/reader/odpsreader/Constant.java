package com.alibaba.datax.plugin.reader.odpsreader;

public class Constant {

    public final static String START_INDEX = "startIndex";

    public final static String STEP_COUNT = "stepCount";

    public final static String SESSION_ID = "sessionId";

    public final static String IS_PARTITIONED_TABLE = "isPartitionedTable";

    public static final String DEFAULT_SPLIT_MODE = "record";

    public static final String PARTITION_SPLIT_MODE = "partition";

    // 常量字段用COLUMN_CONSTANT_FLAG 首尾包住即可
    public final static String COLUMN_CONSTANT_FLAG = "'";

    public static final String PARTITION_COLUMNS = "partitionColumns";
    
    public static final String PARSED_COLUMNS = "parsedColumns";

    public static final String PARTITION_FILTER_HINT =  "/*query*/";

}
