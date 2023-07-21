package com.alibaba.datax.plugin.reader.otsreader.utils;

public class Constant {
    /**
     * Json中的Key名字定义
     */
public class ConfigKey {
        public static final String CONF = "conf";
        public static final String RANGE = "range";
        public static final String META = "meta";
        public static final String SPLIT_INFO = "splitInfo";

        public static final String TIME_RANGE = "timeRange";
        public static final String MAX_VERSION = "maxVersion";
        
        public static final String RETRY = "maxRetryTime";
        public static final String RETRY_PAUSE_IN_MILLISECOND = "retryPauseInMillisecond";
        public static final String IO_THREAD_COUNT = "ioThreadCount";
        public static final String MAX_CONNECTION_COUNT = "maxConnectionCount";
        public static final String SOCKET_TIMEOUTIN_MILLISECOND = "socketTimeoutInMillisecond";
        public static final String CONNECT_TIMEOUT_IN_MILLISECOND = "connectTimeoutInMillisecond";

        public class Range {
            public static final String BEGIN = "begin";
            public static final String END = "end";
            public static final String SPLIT = "split";
        };

        public class PrimaryKeyColumn {
            public static final String TYPE = "type";
            public static final String VALUE = "value";
        };

        public class TimeseriesPKColumn {
            public static final String MEASUREMENT_NAME = "_m_name";
            public static final String DATA_SOURCE = "_data_source";
            public static final String TAGS = "_tags";
            public static final String TIME = "_time";
        }
        
        public class Column {
            public static final String NAME = "name";
            public static final String TYPE = "type";
            public static final String VALUE = "value";
            public static final String IS_TAG = "is_timeseries_tag";
        };
        
        public class TimeRange {
            public static final String BEGIN = "begin";
            public static final String END = "end";
        }
    };
    
    /**
     * 定义的配置文件中value type中可取的值
     */
    public class ValueType {
        public static final String INF_MIN = "INF_MIN";
        public static final String INF_MAX = "INF_MAX";
        public static final String STRING = "string";
        public static final String INTEGER = "int";
        public static final String BINARY = "binary";
        public static final String DOUBLE = "double";
        public static final String BOOLEAN = "bool";
    };
    
    /**
     * 全局默认常量定义
     */
    public class ConfigDefaultValue {
        public static final int RETRY = 18;
        public static final int RETRY_PAUSE_IN_MILLISECOND = 100;
        public static final int IO_THREAD_COUNT = 1;
        public static final int MAX_CONNECTION_COUNT = 1;
        public static final int SOCKET_TIMEOUT_IN_MILLISECOND = 10000;
        public static final int CONNECT_TIMEOUT_IN_MILLISECOND = 10000;
        
        public static final int MAX_VERSION = Integer.MAX_VALUE;

        public static final String DEFAULT_NAME = "DEFAULT_NAME";
        
        public class Mode {
            public static final String NORMAL = "normal";
            public static final String MULTI_VERSION = "multiVersion";
        }
        
        public class TimeRange {
            public static final long MIN = 0;
            public static final long MAX = Long.MAX_VALUE;
        }
    }
}
