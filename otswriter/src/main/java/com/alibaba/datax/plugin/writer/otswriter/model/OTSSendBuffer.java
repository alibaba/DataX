package com.alibaba.datax.plugin.writer.otswriter.model;

import com.alibaba.datax.plugin.writer.otswriter.OTSCriticalException;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class OTSSendBuffer {
    
    private OTSConf conf = null;
    private OTSTaskManagerInterface manager = null;
    
    private int totalSize = 0;
    private List<OTSLine> buffer = new ArrayList<OTSLine>();
    
    
    private static final Logger LOG = LoggerFactory.getLogger(OTSSendBuffer.class);
    
    public OTSSendBuffer(
            SyncClientInterface ots,
            OTSConf conf) {
        this.conf = conf;
        if (conf.isTimeseriesTable()){
            this.manager = new OTSTimeseriesRowTaskManager(ots, conf);
        }
        else {
            this.manager = new OTSBatchWriteRowTaskManager(ots, conf);
        }

    }
    
    public void write(OTSLine line) throws OTSCriticalException {
        LOG.debug("write begin");
        // 检查是否满足发送条件
        if (buffer.size() >= conf.getBatchWriteCount() ||
                ((totalSize + line.getDataSize()) > conf.getRequestTotalSizeLimitation() && totalSize > 0)
        ) {
            try {
                manager.execute(new ArrayList<OTSLine>(buffer));
            } catch (Exception e) {
                LOG.error("OTSBatchWriteRowTaskManager execute fail : {}", e.getMessage(), e);
                throw new OTSCriticalException(e);
            }
            buffer.clear();
            totalSize = 0;
        }
        buffer.add(line);
        totalSize += line.getDataSize();
        LOG.debug("write end");
    }
    
    public void flush() throws OTSCriticalException {
        LOG.debug("flush begin");
        if (!buffer.isEmpty()) {
            try {
                manager.execute(new ArrayList<OTSLine>(buffer));
            } catch (Exception e) {
                LOG.error("OTSBatchWriteRowTaskManager flush fail : {}", e.getMessage(), e);
                throw new OTSCriticalException(e);
            }
        }
        LOG.debug("flush end");
    }
    
    public void close() throws OTSCriticalException {
        LOG.debug("close begin");
        try {
            flush();
        } finally {
            try {
                manager.close();
            } catch (Exception e) {
                LOG.error("OTSBatchWriteRowTaskManager close fail : {}", e.getMessage(), e);
                throw new OTSCriticalException(e);
            }
        }
        LOG.debug("close end");
    }
}
