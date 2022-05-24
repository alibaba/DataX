package com.alibaba.datax.core.transport.exchanger;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WaitedBufferedRecordExchanger extends BufferedRecordExchanger {

    private static final Logger LOG = LoggerFactory
            .getLogger(WaitedBufferedRecordExchanger.class);


    public WaitedBufferedRecordExchanger(Channel channel, TaskPluginCollector pluginCollector) {
        super(channel, pluginCollector);
    }


    @Override
    public void flush() {
        LOG.debug("WaitedBufferedRecordExchanger flush start...");
        super.flush();
        boolean waiting = true;
        while (waiting) {
            if (super.ifChannelEmpty()) {
                LOG.debug("flush  ifChannelEmpty...");
                waiting = false;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        LOG.debug("WaitedBufferedRecordExchanger flush end...");
    }

    /**
     * @param waitTimeOut 单位秒
     */
    @Override
    public void flush(long waitTimeOut) {
        LOG.debug("WaitedBufferedRecordExchanger flush(waitTime) start...");
        super.flush();
        long startFlushTime = System.currentTimeMillis();
        boolean waiting = true;
        while (waiting) {
            if (isWaitTimeout(startFlushTime, waitTimeOut*1000)) {
                throw DataXException.asDataXException(FrameworkErrorCode.KILL_JOB_TIMEOUT_ERROR, "kafkareader等待写入数据库超时");
            }
            if (super.ifChannelEmpty()) {
                LOG.debug("flush long waitTimeOut ifChannelEmpty...");
                waiting = false;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        LOG.debug("WaitedBufferedRecordExchanger flush(waitTime) end...");
    }


    private boolean isWaitTimeout(long startFlushTime, long waitTimeOut) {
        LOG.debug("startFlushTime:"+startFlushTime);
        LOG.debug("waitFlushTime:"+(System.currentTimeMillis() - startFlushTime));
        return System.currentTimeMillis() - startFlushTime > waitTimeOut;
    }


}
