package com.alibaba.datax.plugin.reader.otsstreamreader.internal.config;

import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.OTSErrorCode;
import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

import java.util.Arrays;
import java.util.List;

public class OTSRetryStrategyForStreamReader implements RetryStrategy {

    private int maxRetries = 30;
    private static long retryPauseScaleTimeMillis = 100;
    private static long maxPauseTimeMillis = 10 * 1000;
    private int retries = 0;

    private static List<String> noRetryErrorCode = Arrays.asList(
            OTSErrorCode.AUTHORIZATION_FAILURE,
            OTSErrorCode.CONDITION_CHECK_FAIL,
            OTSErrorCode.INVALID_PARAMETER,
            OTSErrorCode.INVALID_PK,
            OTSErrorCode.OBJECT_ALREADY_EXIST,
            OTSErrorCode.OBJECT_NOT_EXIST,
            OTSErrorCode.OUT_OF_COLUMN_COUNT_LIMIT,
            OTSErrorCode.OUT_OF_ROW_SIZE_LIMIT,
            OTSErrorCode.REQUEST_TOO_LARGE,
            OTSErrorCode.TRIMMED_DATA_ACCESS
    );

    private boolean canRetry(Exception ex) {
        if (ex instanceof TableStoreException) {
            if (noRetryErrorCode.contains(((TableStoreException) ex).getErrorCode())) {
                return false;
            }
            return true;
        } else if (ex instanceof ClientException) {
            return true;
        } else {
            return false;
        }
    }

    public boolean shouldRetry(String action, Exception ex, int retries) {
        if (retries > maxRetries) {
            return false;
        }
        if (canRetry(ex)) {
            return true;
        }
        return false;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public int getMaxRetries() {
        return this.maxRetries;
    }

    @Override
    public RetryStrategy clone() {
        return new OTSRetryStrategyForStreamReader();
    }

    @Override
    public int getRetries() {
        return retries;
    }

    @Override
    public long nextPause(String action, Exception ex) {
        if (!shouldRetry(action, ex, retries)) {
            return 0;
        }

        long pause = Math.min((int)Math.pow(2, retries) * retryPauseScaleTimeMillis, maxPauseTimeMillis);
        ++retries;
        return pause;
    }
}
