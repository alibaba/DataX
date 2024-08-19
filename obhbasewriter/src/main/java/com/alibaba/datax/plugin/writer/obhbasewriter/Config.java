package com.alibaba.datax.plugin.writer.obhbasewriter;

public interface Config {

    String MEMSTORE_THRESHOLD = "memstoreThreshold";

    double DEFAULT_MEMSTORE_THRESHOLD = 0.9d;

    String MEMSTORE_CHECK_INTERVAL_SECOND = "memstoreCheckIntervalSecond";

    long DEFAULT_MEMSTORE_CHECK_INTERVAL_SECOND = 30;

    String FAIL_TRY_COUNT = "failTryCount";

    int DEFAULT_FAIL_TRY_COUNT = 10000;

    String WRITER_THREAD_COUNT = "writerThreadCount";

    int DEFAULT_WRITER_THREAD_COUNT = 5;

    String CONCURRENT_WRITE = "concurrentWrite";

    boolean DEFAULT_CONCURRENT_WRITE = true;

    String RS_URL = "rsUrl";

    String OB_VERSION = "obVersion";

    String TIMEOUT = "timeout";

    String PRINT_COST = "printCost";

    boolean DEFAULT_PRINT_COST = false;

    String COST_BOUND = "costBound";

    long DEFAULT_COST_BOUND = 20;

    String MAX_ACTIVE_CONNECTION = "maxActiveConnection";

    int DEFAULT_MAX_ACTIVE_CONNECTION = 2000;
}
