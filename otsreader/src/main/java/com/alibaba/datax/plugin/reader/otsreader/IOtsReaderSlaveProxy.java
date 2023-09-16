package com.alibaba.datax.plugin.reader.otsreader;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;

/**
 * OTS Reader工作进程接口
 */
public interface IOtsReaderSlaveProxy {
    /**
     * 初始化函数，解析配置、初始化相关资源
     */
    public void init(Configuration configuration);
    
    /**
     * 关闭函数，释放资源
     */
    public void close();
    
    /**
     * 数据导出函数
     * @param recordSender
     * @throws Exception 
     */
    public void startRead(RecordSender recordSender) throws Exception;
}
