package com.alibaba.datax.plugin.reader.tablestorereader.utils;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class RetryHelper {

    private static final Logger LOG = LoggerFactory.getLogger(RetryHelper.class);

    public static <V> V executeWithRetry(Callable<V> callable, int maxRetryTimes, int sleepInMilliSecond) throws Exception {
        int retryTimes = 0;
        while (true) {
            Thread.sleep(Common.getDelaySendMillinSeconds(retryTimes, sleepInMilliSecond));
            try {
                return callable.call();
            } catch (Exception e) {
                LOG.warn("Call callable fail, {}", e.getMessage());
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

    private static boolean canRetry(Exception exception) {

        TableStoreException e = null;
        if (exception instanceof TableStoreException) {
            e = (TableStoreException) exception;
            LOG.warn(
                    "OTSException:ErrorCode:{}, ErrorMsg:{}, RequestId:{}",
                    new Object[]{e.getErrorCode(), e.getMessage(), e.getRequestId()}
            );

            return true;
        } else if (exception instanceof ClientException) {
            ClientException ce = (ClientException) exception;
            LOG.warn(
                    "ClientException:{}, ErrorMsg:{}",
                    new Object[]{ce, ce.getMessage()}
            );
            return true;
        } else {
            return false;
        }
    }
}
