package com.alibaba.datax.plugin.writer.tablestorewriter;

import com.alibaba.datax.plugin.writer.tablestorewriter.model.TableStoreConfig;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

import java.util.Random;

public class WriterRetryPolicy implements RetryStrategy {
    private TableStoreConfig tableStoreConfig;

    private Random rnd = new Random();
    private int base = 4; // in msec
    private int retries = 0;
    private int maxRetryTimes = 3;
    private int maxRetryPauseInMillis = 1000; // one second

    public WriterRetryPolicy(int maxRetryTimes, int maxRetryPauseInMillis) {
        Preconditions.checkArgument(maxRetryTimes > 0);
        Preconditions.checkArgument(maxRetryPauseInMillis > 1);

        this.maxRetryTimes = maxRetryTimes;
        this.maxRetryPauseInMillis = maxRetryPauseInMillis;
    }

    @Override
    public WriterRetryPolicy clone() {
        return new WriterRetryPolicy(maxRetryTimes, maxRetryPauseInMillis);
    }


    @Override
    public int getRetries() {
        return retries;
    }

    @Override
    public long nextPause(String action, Exception ex) {
        if (retries >= maxRetryTimes) {
            return 0;
        }
        if (base <= 0) {
            return 0;
        }

        int maxPause = 0;
        if (base * 2 < maxRetryPauseInMillis) {
            base *= 2;
            maxPause = base;
        } else {
            maxPause = maxRetryPauseInMillis;
        }
        int halfPause = maxPause / 2;
        // randomly exponential backoff, in order to make requests sparse.
        long delay = halfPause + rnd.nextInt(maxPause - halfPause);
        ++retries;
        return delay;
    }
}
