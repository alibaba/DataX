package com.alibaba.datax.plugin.writer.adswriter.util;

public class Constant {

    public static final String LOADMODE = "load";

    public static final String INSERTMODE = "insert";
    
    public static final String DELETEMODE = "delete";

    public static final String REPLACEMODE = "replace";
    
    public static final String STREAMMODE = "stream";

    public static final int DEFAULT_BATCH_SIZE = 32;
    
    public static final long DEFAULT_SOCKET_TIMEOUT = 3600000L;
    
    public static final int DEFAULT_RETRY_TIMES = 3;
    
    public static final String INSERT_TEMPLATE = "insert into %s ( %s ) values ";
    
    public static final String DELETE_TEMPLATE = "delete from %s where ";
    
    public static final String ADS_TABLE_INFO = "adsTableInfo";
    
    public static final String ADS_QUOTE_CHARACTER = "`";
    
}
