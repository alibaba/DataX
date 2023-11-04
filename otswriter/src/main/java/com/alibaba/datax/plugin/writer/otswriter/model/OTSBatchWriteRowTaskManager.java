package com.alibaba.datax.plugin.writer.otswriter.model;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 控制Task的并发数目
 *
 */
public class OTSBatchWriteRowTaskManager implements OTSTaskManagerInterface {

    private SyncClientInterface ots = null;
    private OTSBlockingExecutor executorService = null;
    private OTSConf conf = null;

    private static final Logger LOG = LoggerFactory.getLogger(OTSBatchWriteRowTaskManager.class);

    public OTSBatchWriteRowTaskManager(
            SyncClientInterface ots,
            OTSConf conf) {
        this.ots = ots;
        this.conf = conf;
        
        executorService = new OTSBlockingExecutor(conf.getConcurrencyWrite());
    }

    public void execute(List<OTSLine> lines) throws Exception {
        LOG.debug("Begin execute.");
        executorService.execute(new OTSBatchWriterRowTask(ots, conf, lines));
        LOG.debug("End execute.");
    }

    public void close() throws Exception {
        LOG.debug("Begin close.");
        executorService.shutdown();
        LOG.debug("End close.");
    }
}
