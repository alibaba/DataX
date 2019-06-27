package com.alibaba.datax.plugin.writer.tablestorewriter.utils;

import com.alibaba.datax.plugin.writer.tablestorewriter.model.LogExceptionManager;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

public class RetryHelper {

    private static final Logger LOG = LoggerFactory.getLogger(RetryHelper.class);
    private static final Set<String> noRetryErrorCode = prepareNoRetryErrorCode();

    public static LogExceptionManager logManager = new LogExceptionManager();

    public static <V> V executeWithRetry(Callable<V> callable, int maxRetryTimes, int sleepInMilliSecond) throws Exception {
        int retryTimes = 0;
        while (true) {
            Thread.sleep(Common.getDelaySendMilliseconds(retryTimes, sleepInMilliSecond));
            try {
                return callable.call();
            } catch (Exception e) {
                logManager.addException(e);
                if (!canRetry(e)) {
                    LOG.error("Can not retry for Exception.", e);
                    throw e;
                } else if (retryTimes >= maxRetryTimes) {
                    LOG.error("Retry times more than limition. maxRetryTimes : {}", maxRetryTimes);
                    throw e;
                }
                retryTimes++;
                LOG.warn("Retry time : {}", retryTimes);
            }
        }
    }

    private static Set<String> prepareNoRetryErrorCode() {
        Set<String> pool = new HashSet<String>();
        pool.add(ErrorCode.AUTHORIZATION_FAILURE);
        pool.add(ErrorCode.INVALID_PARAMETER);
        pool.add(ErrorCode.REQUEST_TOO_LARGE);
        pool.add(ErrorCode.OBJECT_NOT_EXIST);
        pool.add(ErrorCode.OBJECT_ALREADY_EXIST);
        pool.add(ErrorCode.INVALID_PK);
        pool.add(ErrorCode.OUT_OF_COLUMN_COUNT_LIMIT);
        pool.add(ErrorCode.OUT_OF_ROW_SIZE_LIMIT);
        pool.add(ErrorCode.CONDITION_CHECK_FAIL);
        return pool;
    }

    private static boolean canRetry(String tableStoreErrorCode) {
        if (noRetryErrorCode.contains(tableStoreErrorCode)) {
            return false;
        } else {
            return true;
        }
    }

    private static boolean canRetry(Exception exception) {
        TableStoreException tableStoreException = null;
        if (exception instanceof TableStoreException) {
            tableStoreException = (TableStoreException) exception;
            return canRetry(tableStoreException.getErrorCode());
        } else if (exception instanceof ClientException) {
            return true;
        } else {
            return false;
        }
    }
}
