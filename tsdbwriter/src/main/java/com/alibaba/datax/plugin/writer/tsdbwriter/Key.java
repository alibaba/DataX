package com.alibaba.datax.plugin.writer.tsdbwriter;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：Key
 *
 * @author Benedict Jin
 * @since 2019-04-18
 */
public class Key {

    static final String SOURCE_DB_TYPE = "sourceDbType";
    static final String MULTI_FIELD = "multiField";

    // common
    static final String ENDPOINT = "endpoint";
    static final String USERNAME = "username";
    static final String PASSWORD = "password";
    static final String IGNORE_WRITE_ERROR = "ignoreWriteError";
    static final String DATABASE = "database";

    // for tsdb
    static final String BATCH_SIZE = "batchSize";
    static final String MAX_RETRY_TIME = "maxRetryTime";

    // for rdb
    static final String COLUMN = "column";
    static final String COLUMN_TYPE = "columnType";
    static final String TABLE = "table";
}
