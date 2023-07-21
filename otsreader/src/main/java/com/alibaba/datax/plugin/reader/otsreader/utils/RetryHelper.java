package com.alibaba.datax.plugin.reader.otsreader.utils;

import com.alibaba.datax.plugin.reader.otsreader.model.OTSErrorCode;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

public class RetryHelper {
    
    private static final Logger LOG = LoggerFactory.getLogger(RetryHelper.class);
    private static final Set<String> noRetryErrorCode = prepareNoRetryErrorCode();
    
    public static <V> V executeWithRetry(Callable<V> callable, int maxRetryTimes, int sleepInMilliSecond) throws Exception {
        int retryTimes = 0;
        while (true){
            Thread.sleep(getDelaySendMillinSeconds(retryTimes, sleepInMilliSecond));
            try {
                return callable.call();
            } catch (Exception e) {
                LOG.warn("Call callable fail, {}", e.getMessage());
                if (!canRetry(e)){
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
        pool.add(OTSErrorCode.AUTHORIZATION_FAILURE);
        pool.add(OTSErrorCode.INVALID_PARAMETER);
        pool.add(OTSErrorCode.REQUEST_TOO_LARGE);
        pool.add(OTSErrorCode.OBJECT_NOT_EXIST);
        pool.add(OTSErrorCode.OBJECT_ALREADY_EXIST);
        pool.add(OTSErrorCode.INVALID_PK);
        pool.add(OTSErrorCode.OUT_OF_COLUMN_COUNT_LIMIT);
        pool.add(OTSErrorCode.OUT_OF_ROW_SIZE_LIMIT);
        pool.add(OTSErrorCode.CONDITION_CHECK_FAIL);
        return pool;
    }
    
    public static boolean canRetry(String otsErrorCode) {
        if (noRetryErrorCode.contains(otsErrorCode)) {
            return false;
        } else {
            return true;
        }
    }
    
    public static boolean canRetry(Exception exception) {
        TableStoreException e = null;
        if (exception instanceof TableStoreException) {
            e = (TableStoreException) exception;
            LOG.warn(
                    "OTSException:ErrorCode:{}, ErrorMsg:{}, RequestId:{}", 
                    new Object[]{e.getErrorCode(), e.getMessage(), e.getRequestId()}
                    );
            return canRetry(e.getErrorCode());

        } else if (exception instanceof ClientException) {
            ClientException ce = (ClientException) exception;
            LOG.warn(
                    "ClientException:{}", 
                    new Object[]{ce.getMessage()}
                    );
            return true;
        } else {
            return false;
        } 
    }
    
    public static long getDelaySendMillinSeconds(int hadRetryTimes, int initSleepInMilliSecond) {

        if (hadRetryTimes <= 0) {
            return 0;
        }

        int sleepTime = initSleepInMilliSecond;
        for (int i = 1; i < hadRetryTimes; i++) {
            sleepTime += sleepTime;
            if (sleepTime > 30000) {
                sleepTime = 30000;
                break;
            } 
        }
        return sleepTime;
    }
}
