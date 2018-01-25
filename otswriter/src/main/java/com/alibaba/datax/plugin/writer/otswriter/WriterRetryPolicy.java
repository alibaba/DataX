package com.alibaba.datax.plugin.writer.otswriter;

import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.aliyun.openservices.ots.internal.OTSRetryStrategy;

public class WriterRetryPolicy implements OTSRetryStrategy {
    OTSConf conf;

    public WriterRetryPolicy(OTSConf conf) {
        this.conf = conf;
    }

    @Override
    public boolean shouldRetry(String action, Exception ex, int retries) {
        return retries <= conf.getRetry();
    }

    @Override
    public long getPauseDelay(String action, Exception ex, int retries) {
        if (retries <= 0) {
            return 0;
        }

        int sleepTime = conf.getSleepInMillisecond() * retries;
        return sleepTime > 30000 ? 30000 : sleepTime;
    }
}
