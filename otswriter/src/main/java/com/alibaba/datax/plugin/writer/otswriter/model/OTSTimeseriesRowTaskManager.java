package com.alibaba.datax.plugin.writer.otswriter.model;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.TimeseriesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class OTSTimeseriesRowTaskManager implements OTSTaskManagerInterface{

    private TimeseriesClient client = null;
    private OTSBlockingExecutor executorService = null;
    private OTSConf conf = null;

    private static final Logger LOG = LoggerFactory.getLogger(OTSTimeseriesRowTaskManager.class);

    public OTSTimeseriesRowTaskManager(
            SyncClientInterface ots,
            OTSConf conf) {
        this.client = ((SyncClient)ots).asTimeseriesClient();
        this.conf = conf;

        executorService = new OTSBlockingExecutor(conf.getConcurrencyWrite());
    }

    @Override
    public void execute(List<OTSLine> lines) throws Exception {
        LOG.debug("Begin execute.");
        executorService.execute(new OTSTimeseriesRowTask(client, conf, lines));
        LOG.debug("End execute.");
    }

    @Override
    public void close() throws Exception {
        LOG.debug("Begin close.");
        executorService.shutdown();
        LOG.debug("End close.");
    }
}
