package com.dai;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.RetryUtil;
import com.q1.datax.plugin.writer.kudu11xwriter.*;
import static org.apache.kudu.client.AsyncKuduClient.LOG;

/**
 * @author daizihao
 * @create 2020-08-28 11:03
 **/
public class test {
    static boolean isSkipFail;


    public static void main(String[] args) {
        try {
            while (true) {
                try {
                    RetryUtil.executeWithRetry(()->{
                        throw new RuntimeException();
                    },5,1000L,true);

                } catch (Exception e) {
                    LOG.error("Data write failed!", e);
                    System.out.println(isSkipFail);
                    if (isSkipFail) {
                        LOG.warn("Because you have configured skipFail is true,this data will be skipped!");
                    }else {
                        System.out.println("异常抛出");
                        throw e;
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("write failed! the task will exit!");
            throw DataXException.asDataXException(Kudu11xWriterErrorcode.PUT_KUDU_ERROR, e);
        }
    }
}
