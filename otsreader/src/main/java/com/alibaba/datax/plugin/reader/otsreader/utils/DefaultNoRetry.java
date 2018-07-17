package com.alibaba.datax.plugin.reader.otsreader.utils;

import com.aliyun.openservices.ots.internal.OTSDefaultRetryStrategy;

public class DefaultNoRetry extends OTSDefaultRetryStrategy {

    @Override
    public boolean shouldRetry(String action, Exception ex, int retries) {
        return false;
    }

}