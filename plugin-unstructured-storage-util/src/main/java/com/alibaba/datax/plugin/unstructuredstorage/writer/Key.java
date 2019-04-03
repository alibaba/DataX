package com.alibaba.datax.plugin.unstructuredstorage.writer;

public class Key {
    // must have
    public static final String FILE_NAME = "fileName";

    // must have
    public static final String WRITE_MODE = "writeMode";

    // not must , not default ,
    public static final String FIELD_DELIMITER = "fieldDelimiter";

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
    
    // writer file type suffix, like .txt  .csv
    public static final String SUFFIX = "suffix";
}
