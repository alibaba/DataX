package com.alibaba.datax.plugin.writer.otswriter.utils;

import com.alibaba.datax.plugin.writer.otswriter.OTSErrorCode;
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
    
    /**
     * 对重试的封装，方法需要用户传入最大重试次数，最大的重试时间。
     * 如果方法执行失败，方法会进入重试，每次重试之前，方法会sleep一段时间（sleep机制请参见
     * Common.getDelaySendMillinSeconds方法），直到重试次数达到上限，系统会抛出异常。
     * @param callable
     * @param maxRetryTimes
     * @param sleepInMilliSecond
     * @return
     * @throws Exception
     */
    public static <V> V executeWithRetry(Callable<V> callable, int maxRetryTimes, int sleepInMilliSecond) throws Exception {
        int retryTimes = 0;
        while (true){
            Thread.sleep(Common.getDelaySendMillinSeconds(retryTimes, sleepInMilliSecond));
            try {
                return callable.call();
            } catch (Exception e) {
                LOG.warn("Call callable fail.", e);
                if (!canRetry(e)){
                    LOG.error("Can not retry for Exception : {}", e.getMessage()); 
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
        final Set<String> pool = new HashSet<String>();
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
                    "ClientException:ErrorMsg:{}", 
                    ce.getMessage()
                    );
            return true;
        } else {
            return false;
        } 
    }
}
