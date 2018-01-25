package com.alibaba.datax.plugin.writer.otswriter.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.openservices.ots.OTSErrorCode;
import com.aliyun.openservices.ots.OTSException;

/**
 * 添加这个类的主要目的是为了解决当用户遇到CU不够时，打印大量的日志
 * @author redchen
 *
 */
public class LogExceptionManager {
    
    private long count = 0;
    private long updateTimestamp = 0;
    
    private static final Logger LOG = LoggerFactory.getLogger(LogExceptionManager.class);
    
    private synchronized void countAndReset() {
        count++;
        long cur = System.currentTimeMillis();
        long interval = cur - updateTimestamp;
        if (interval >= 10000) {
            LOG.warn("Call callable fail, OTSNotEnoughCapacityUnit, total times:"+ count +", time range:"+ (interval/1000) +"s, times per second:" + ((float)count / (interval/1000)));
            count = 0;
            updateTimestamp = cur;
        }
    }
    
    public synchronized void addException(Exception exception) {
        if (exception instanceof OTSException) {
            OTSException e = (OTSException)exception;
            if (e.getErrorCode().equals(OTSErrorCode.NOT_ENOUGH_CAPACITY_UNIT)) {
                countAndReset();
            } else {
                LOG.warn(
                        "Call callable fail, OTSException:ErrorCode:{}, ErrorMsg:{}, RequestId:{}", 
                        new Object[]{e.getErrorCode(), e.getMessage(), e.getRequestId()}
                        );
            }
        } else {
            LOG.warn("Call callable fail, {}", exception.getMessage());
        }
    }
    
    public synchronized void addException(com.aliyun.openservices.ots.model.Error error, String requestId) {
        if (error.getCode().equals(OTSErrorCode.NOT_ENOUGH_CAPACITY_UNIT)) {
            countAndReset();
        } else {
            LOG.warn(
                    "OTSException:ErrorCode:{}, ErrorMsg:{}, RequestId:{}", 
                    new Object[]{error.getCode(), error.getMessage(), requestId}
                    );
        }
    }
}
