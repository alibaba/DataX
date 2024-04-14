package com.alibaba.datax.plugin.writer.otswriter;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;

public interface IOtsWriterSlaveProxy {
    
    /**
     * Slave的初始化，创建Slave所使用的资源
     */
    public void init(Configuration configuration);
    
    /**
     * 释放Slave的所有资源
     */
    public void close() throws OTSCriticalException;
    
    /**
     * Slave的执行器，将Datax的数据写入到OTS中
     * @param recordReceiver
     * @throws OTSCriticalException
     */
    public void write(RecordReceiver recordReceiver, TaskPluginCollector taskPluginCollector) throws OTSCriticalException;
}
