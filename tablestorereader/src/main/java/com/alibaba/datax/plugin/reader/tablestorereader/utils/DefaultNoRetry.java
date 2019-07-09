package com.alibaba.datax.plugin.reader.tablestorereader.utils;

public class DefaultNoRetry {

    public boolean shouldRetry(String action, Exception ex, int retries) {
        return false;
    }
}