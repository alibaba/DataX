package com.alibaba.datax.plugin.writer.otswriter.model;

public class OTSErrorMessage {
    
    public static final String MODE_PARSE_ERROR = "The 'mode' only support 'normal' and 'multiVersion' not '%s'.";
    
    public static final String OPERATION_PARSE_ERROR = "The 'writeMode' only support 'PutRow' and 'UpdateRow' not '%s'.";
    
    public static final String MUTLI_MODE_OPERATION_PARSE_ERROR = "When configurion set mode='MultiVersion', the 'writeMode' only support 'UpdateRow' not '%s'.";
    
    public static final String UNSUPPORT_PARSE = "Unsupport parse '%s' to '%s'.";
    
    public static final String UNSUPPORT = "Unsupport : '%s'.";
    
    public static final String RECORD_AND_COLUMN_SIZE_ERROR = "Size of record not equal size of config column. record size : %d, config column size : %d, record data : %s.";
    
    public static final String PK_TYPE_ERROR = "Primary key type only support 'string', 'int' and 'binary', not support '%s'.";
    
    public static final String ATTR_TYPE_ERROR = "Column type only support 'string','int','double','bool' and 'binary', not support '%s'.";
    
    public static final String PK_COLUMN_MISSING_ERROR = "Missing the column '%s' in 'primaryKey'.";
    
    public static final String INPUT_PK_COUNT_NOT_EQUAL_META_ERROR = "The count of 'primaryKey' not equal meta, input count : %d, primary key count : %d in meta.";
    
    public static final String INPUT_PK_TYPE_NOT_MATCH_META_ERROR = "The type of 'primaryKey' not match meta, column name : %s, input type: %s, primary key type : %s in meta.";

    public static final String INPUT_PK_NAME_NOT_EXIST_IN_META_ERROR = "The input primary column '%s' is not exist in meta.";

    public static final String ATTR_REPEAT_COLUMN_ERROR = "Repeat column '%s' in 'column'.";
    
    public static final String MISSING_PARAMTER_ERROR = "The param '%s' is not exist.";
    
    public static final String PARAMTER_STRING_IS_EMPTY_ERROR = "The param length of '%s' is zero.";
    
    public static final String PARAMETER_LIST_IS_EMPTY_ERROR  = "The param '%s' is a empty json array.";
    
    public static final String PARAMETER_IS_NOT_ARRAY_ERROR  = "The param '%s' is not a json array.";
    
    public static final String PARAMETER_IS_NOT_MAP_ERROR  = "The param '%s' is not a json map.";
    
    public static final String PARSE_TO_LIST_ERROR = "Can not parse '%s' to list.";
    
    public static final String PK_MAP_NAME_TYPE_ERROR = "The 'name' and 'type only support string in json map of 'primaryKey'.";
    
    public static final String ATTR_MAP_NAME_TYPE_ERROR = "The 'name' and 'type only support string in json map of 'column'.";
    
    public static final String ATTR_MAP_SRCNAME_NAME_TYPE_ERROR = "The 'srcName', 'name' and 'type' only support string in json map of 'column'.";
    
    public static final String PK_MAP_KEY_TYPE_ERROR = "The '%s' only support string in json map of 'primaryKey'.";
    
    public static final String ATTR_MAP_KEY_TYPE_ERROR = "The '%s' only support string in json map of 'column'.";
    
    public static final String PK_MAP_INCLUDE_NAME_TYPE_ERROR = "The only support 'name' and 'type' fileds in json map of 'primaryKey'.";
    
    public static final String ATTR_MAP_INCLUDE_NAME_TYPE_ERROR = "The only support 'name' and 'type' fileds in json map of 'column'.";
    
    public static final String PK_MAP_FILED_MISSING_ERROR = "The '%s' fileds is missing in json map of 'primaryKey'.";
    
    public static final String ATTR_MAP_FILED_MISSING_ERROR = "The '%s' fileds is missing in json map of 'column'.";
    
    public static final String ATTR_MAP_INCLUDE_SRCNAME_NAME_TYPE_ERROR = "The only support 'srcName', 'name' and 'type' fileds in json map of 'column'.";
    
    public static final String PK_ITEM_IS_ILLEAGAL_ERROR = "The item is not string or map in 'primaryKey'.";
    
    public static final String PK_IS_NOT_EXIST_AT_OTS_ERROR = "Can not find the pk('%s') at ots in 'primaryKey'.";
    
    public static final String ATTR_ITEM_IS_NOT_MAP_ERROR = "The item is not map in 'column'.";
    
    public static final String PK_COLUMN_NAME_IS_EMPTY_ERROR = "The name of item can not be a empty string in 'primaryKey'.";
    
    public static final String PK_COLUMN_TYPE_IS_EMPTY_ERROR = "The type of item can not be a empty string in 'primaryKey'.";
    
    public static final String ATTR_COLUMN_NAME_IS_EMPTY_ERROR = "The name of item can not be a empty string in 'column'.";
    
    public static final String ATTR_COLUMN_SRC_NAME_IS_EMPTY_ERROR = "The srcName of item can not be a empty string in 'column'.";
    
    public static final String ATTR_COLUMN_TYPE_IS_EMPTY_ERROR = "The type of item can not be a empty string in 'column'.";
    
    public static final String MULTI_PK_ATTR_COLUMN_ERROR = "Duplicate item in 'column' and 'primaryKey', column name : %s .";
    
    public static final String MULTI_ATTR_COLUMN_ERROR = "Duplicate item in 'column', column name : %s .";
    
    public static final String MULTI_ATTR_SRC_COLUMN_ERROR = "Duplicate src name in 'column', src name : %s .";
    
    public static final String COLUMN_CONVERSION_ERROR = "Column coversion error, src type : %s, src value: %s, expect type: %s .";
    
    public static final String PK_COLUMN_VALUE_IS_NULL_ERROR = "The column of record is NULL, primary key name : %s .";
    
    public static final String PK_STRING_LENGTH_ERROR = "The length of pk string value is more than configuration, conf: %d, input: %d .";
    
    public static final String ATTR_STRING_LENGTH_ERROR = "The length of attr string value is more than configuration, conf: %d, input: %d .";
    
    public static final String BINARY_LENGTH_ERROR = "The length of binary value is more than configuration, conf: %d, input: %d .";
    
    public static final String LINE_LENGTH_ERROR = "The length of row is more than length of request configuration, conf: %d, row: %d .";
    
    public static final String INSERT_TASK_ERROR = "Can not execute the task, becase the ExecutorService is shutdown.";
    
    public static final String COLUMN_NOT_DEFINE = "The column name : '%s' not define in column.";
    
    public static final String INPUT_RECORDS_IS_EMPTY = "The input records can not be empty.";
    
    public static final String MULTI_VERSION_TIMESTAMP_IS_EMPTY = "The input timestamp can not be empty in the multiVersion mode.";
    
    public static final String MULTI_VERSION_VALUE_IS_EMPTY = "The input value can not be empty in the multiVersion mode.";
    
    public static final String INPUT_COLUMN_COUNT_LIMIT = "The input count(%d) of column more than max(%d).";

    public static final String PUBLIC_SDK_NO_SUPPORT_MULTI_VERSION = "The old version do not support multi version function. Please add config in otswriter: \"newVersion\":\"true\" .";

    public static final String PUBLIC_SDK_NO_SUPPORT_AUTO_INCREMENT = "The old version do not support auto increment primary key function. Please add config in otswriter: \"newVersion\":\"true\" .";

    public static final String NOT_SUPPORT_MULTI_VERSION_AUTO_INCREMENT = "The multi version mode do not support auto increment primary key function.";

    public static final String PUBLIC_SDK_NO_SUPPORT_TIMESERIES_TABLE = "The old version do not support write timeseries table. Please add config in otswriter: \"newVersion\":\"true\" .";

    public static final String NOT_SUPPORT_TIMESERIES_TABLE_AUTO_INCREMENT = "The timeseries table do not support auto increment primary key function.";

    public static final String NO_FOUND_M_NAME_FIELD_ERROR = "The '_m_name' field should be set in columns because 'measurement' is required in timeseries data.";

    public static final String NO_FOUND_TIME_FIELD_ERROR = "The '_time' field should be set in columns because 'time' is required in timeseries data.";

    public static final String TIMEUNIT_FORMAT_ERROR = "The value of param 'timeunit' is '%s', which should be in ['NANOSECONDS', 'MICROSECONDS', 'MILLISECONDS', 'SECONDS', 'MINUTES'].";

}
