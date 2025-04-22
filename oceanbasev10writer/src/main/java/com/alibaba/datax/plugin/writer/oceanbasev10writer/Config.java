package com.alibaba.datax.plugin.writer.oceanbasev10writer;

public interface Config {

    String MEMSTORE_THRESHOLD = "memstoreThreshold";

    double DEFAULT_MEMSTORE_THRESHOLD = 0.9d;

    double DEFAULT_SLOW_MEMSTORE_THRESHOLD = 0.75d;

    String MEMSTORE_CHECK_INTERVAL_SECOND = "memstoreCheckIntervalSecond";

    long DEFAULT_MEMSTORE_CHECK_INTERVAL_SECOND = 30;

    int DEFAULT_BATCH_SIZE = 100;

    int MAX_BATCH_SIZE = 4096;

    String FAIL_TRY_COUNT = "failTryCount";

    int DEFAULT_FAIL_TRY_COUNT = 10000;

    String WRITER_THREAD_COUNT = "writerThreadCount";

    int DEFAULT_WRITER_THREAD_COUNT = 1;

    String CONCURRENT_WRITE = "concurrentWrite";

    boolean DEFAULT_CONCURRENT_WRITE = true;

    String OB_VERSION = "obVersion";

    String TIMEOUT = "timeout";

    String PRINT_COST = "printCost";

    boolean DEFAULT_PRINT_COST = false;

    String COST_BOUND = "costBound";

    long DEFAULT_COST_BOUND = 20;

    String MAX_ACTIVE_CONNECTION = "maxActiveConnection";

    int DEFAULT_MAX_ACTIVE_CONNECTION = 2000;

    String WRITER_SUB_TASK_COUNT = "writerSubTaskCount";

    int DEFAULT_WRITER_SUB_TASK_COUNT = 1;

    int MAX_WRITER_SUB_TASK_COUNT = 4096;

    String OB_WRITE_MODE = "obWriteMode";

    String OB_COMPATIBLE_MODE = "obCompatibilityMode";

    String OB_COMPATIBLE_MODE_ORACLE = "ORACLE";

    String OB_COMPATIBLE_MODE_MYSQL = "MYSQL";

    String OCJ_GET_CONNECT_TIMEOUT = "ocjGetConnectTimeout";

    int DEFAULT_OCJ_GET_CONNECT_TIMEOUT = 5000; // 5s

    String OCJ_PROXY_CONNECT_TIMEOUT = "ocjProxyConnectTimeout";

    int DEFAULT_OCJ_PROXY_CONNECT_TIMEOUT = 5000; // 5s

    String OCJ_CREATE_RESOURCE_TIMEOUT = "ocjCreateResourceTimeout";

    int DEFAULT_OCJ_CREATE_RESOURCE_TIMEOUT = 60000; // 60s

    String OB_UPDATE_COLUMNS = "obUpdateColumns";

    String USE_PART_CALCULATOR = "usePartCalculator";

    boolean DEFAULT_USE_PART_CALCULATOR = false;

    String BLOCKS_COUNT = "blocksCount";

    String DIRECT_PATH = "directPath";

    String RPC_PORT = "rpcPort";

    // 区别于recordLimit，这个参数仅针对某张表。即一张表超过最大错误数不会影响其他表。仅用于旁路导入。
    String MAX_ERRORS = "maxErrors";
}
