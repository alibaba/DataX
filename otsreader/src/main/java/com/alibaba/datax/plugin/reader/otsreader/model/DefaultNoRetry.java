package com.alibaba.datax.plugin.reader.otsreader.model;


import com.alicloud.openservices.tablestore.model.DefaultRetryStrategy;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class DefaultNoRetry extends DefaultRetryStrategy {

    public DefaultNoRetry() {
        super();
    }

    @Override
    public RetryStrategy clone() {
        return super.clone();
    }

    @Override
    public int getRetries() {
        return super.getRetries();
    }

    @Override
    public boolean shouldRetry(String action, Exception ex) {
        return false;
    }

    @Override
    public long nextPause(String action, Exception ex) {
        return super.nextPause(action, ex);
    }
}