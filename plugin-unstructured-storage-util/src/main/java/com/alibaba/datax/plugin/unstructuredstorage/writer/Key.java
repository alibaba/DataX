package com.alibaba.datax.plugin.unstructuredstorage.writer;

public class Key {
    public static final String PATH = "path";
    // must have
    public static final String FILE_NAME = "fileName";

    public static final String TABLE_NAME = "table";

    // must have
    public static final String WRITE_MODE = "writeMode";

    // not must , not default ,
    public static final String FIELD_DELIMITER = "fieldDelimiter";

    public static final String QUOTE_CHARACTER = "quoteChar";

    // not must , default os's line delimiter
    public static final String LINE_DELIMITER = "lineDelimiter";

    public static final String CSV_WRITER_CONFIG = "csvWriterConfig";

    // not must, default UTF-8
    public static final String ENCODING = "encoding";

    // not must, default no compress
    public static final String COMPRESS = "compress";

    // not must, not default \N
    public static final String NULL_FORMAT = "nullFormat";

    // not must, date format old style, do not use this
    public static final String FORMAT = "format";
    // for writers ' data format
    public static final String DATE_FORMAT = "dateFormat";
    
    // csv or plain text
    public static final String FILE_FORMAT = "fileFormat";
    
    // writer headers
    public static final String HEADER = "header";
    
    // writer maxFileSize
    public static final String MAX_FILE_SIZE = "maxFileSize";

    public static final String COMMIT_SIZE = "commitSize";
    
    // writer file type suffix, like .txt  .csv
    public static final String SUFFIX = "suffix";

    public static final String MARK_DONE_FILE_NAME = "markDoneFileName";

    public static final String MARK_DOING_FILE_NAME = "markDoingFileName";

    // public static final String RETRY_TIME = "retryTime";

    public final static String MAX_RETRY_TIME = "maxRetryTime";

    /**
     * 半结构化标示一个Record来源的绝对文件路径名，可以是ftp文件，oss的object等
     * */
    public static final String META_KEY_FILE_PATH = "filePath";

    /**
     * 多文件切分的工作项，Task通过此配置项表示工作内容, 文件内部切分相关key
     */
    public static final String SPLIT_SLICE_CONFIG = "__splitSliceConfig";
    public static final String SPLIT_SLICE_FILE_PATH = "filePath";
    public static final String SPLIT_SLICE_START_POINT = "startPoint";
    public static final String SPLIT_SLICE_END_POINT = "endPoint";

    /**
     * 文件同步模式， 如果是copy表示纯文件拷贝
     * */
    public static final String SYNC_MODE = "syncMode";

    public static final String BYTE_ENCODING = "byteEncoding";
}
