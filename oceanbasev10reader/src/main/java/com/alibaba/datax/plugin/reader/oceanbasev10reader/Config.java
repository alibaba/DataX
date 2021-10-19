package com.alibaba.datax.plugin.reader.oceanbasev10reader;

public interface Config {
    // queryTimeoutSecond
    String QUERY_TIMEOUT_SECOND = "memstoreCheckIntervalSecond";

    int DEFAULT_QUERY_TIMEOUT_SECOND = 60 * 60 * 48;// 2天

    // readBatchSize
    String READ_BATCH_SIZE = "readBatchSize";

    int DEFAULT_READ_BATCH_SIZE = 100000;// 10万

    String RETRY_LIMIT = "retryLimit";
    int DEFAULT_RETRY_LIMIT = 10;
}
