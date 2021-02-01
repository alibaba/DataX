package com.alibaba.datax.plugin.writer.otswriter;

import static com.alibaba.datax.plugin.writer.otswriter.model.OTSErrorMessage.RECORD_AND_COLUMN_SIZE_ERROR;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSAttrColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConst;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSPKColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.WithRecord;
import com.alibaba.datax.plugin.writer.otswriter.utils.Common;
import com.alibaba.datax.plugin.writer.otswriter.utils.GsonParser;
import com.aliyun.openservices.ots.ClientConfiguration;
import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.DefaultOTSWriter;
import com.aliyun.openservices.ots.OTSAsync;
import com.aliyun.openservices.ots.OTSClientAsync;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.OTSServiceConfiguration;
import com.aliyun.openservices.ots.OTSWriter;
import com.aliyun.openservices.ots.internal.OTSCallback;
import com.aliyun.openservices.ots.internal.writer.WriterConfig;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.ConsumedCapacity;
import com.aliyun.openservices.ots.model.OTSContext;
import com.aliyun.openservices.ots.model.RowChange;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OtsWriterSlaveProxy {

  private static final Logger LOG = LoggerFactory.getLogger(OtsWriterSlaveProxy.class);
  private OTSConf conf;
  private OTSAsync otsAsync;
  private OTSWriter otsWriter;

  private class WriterCallback implements OTSCallback<RowChange, ConsumedCapacity> {

    private TaskPluginCollector collector;

    public WriterCallback(TaskPluginCollector collector) {
      this.collector = collector;
    }

    @Override
    public void onCompleted(OTSContext<RowChange, ConsumedCapacity> otsContext) {
      LOG.debug("Write row succeed. PrimaryKey: {}.",
          otsContext.getOTSRequest().getRowPrimaryKey());
    }

    @Override
    public void onFailed(OTSContext<RowChange, ConsumedCapacity> otsContext, OTSException ex) {
      LOG.error("Write row failed.", ex);
      WithRecord withRecord = (WithRecord) otsContext.getOTSRequest();
      collector.collectDirtyRecord(withRecord.getRecord(), ex);
    }

    @Override
    public void onFailed(OTSContext<RowChange, ConsumedCapacity> otsContext, ClientException ex) {
      LOG.error("Write row failed.", ex);
      WithRecord withRecord = (WithRecord) otsContext.getOTSRequest();
      collector.collectDirtyRecord(withRecord.getRecord(), ex);
    }
  }

  public void init(Configuration configuration) {
    conf = GsonParser.jsonToConf(configuration.getString(OTSConst.OTS_CONF));

    ClientConfiguration clientConfigure = new ClientConfiguration();
    clientConfigure.setIoThreadCount(conf.getIoThreadCount());
    clientConfigure.setMaxConnections(conf.getConcurrencyWrite());
    clientConfigure.setSocketTimeoutInMillisecond(conf.getSocketTimeout());
    clientConfigure.setConnectionTimeoutInMillisecond(conf.getConnectTimeout());

    OTSServiceConfiguration otsConfigure = new OTSServiceConfiguration();
    otsConfigure.setRetryStrategy(new WriterRetryPolicy(conf));

    otsAsync = new OTSClientAsync(
        conf.getEndpoint(),
        conf.getAccessId(),
        conf.getAccessKey(),
        conf.getInstanceName(),
        clientConfigure,
        otsConfigure);
  }

  public void close() {
    otsAsync.shutdown();
  }

  public void write(RecordReceiver recordReceiver, TaskPluginCollector collector) {
    LOG.info("Writer slave started.");

    WriterConfig writeCfg = new WriterConfig();
    writeCfg.setConcurrency(conf.getConcurrencyWrite());
    writeCfg.setMaxBatchRowsCount(conf.getBatchWriteCount());
    writeCfg.setMaxBatchSize(conf.getRestrictConf().getRequestTotalSizeLimition());
    writeCfg.setBufferSize(conf.getBufferSize());
    writeCfg.setMaxAttrColumnSize(conf.getRestrictConf().getAttributeColumnSize());
    writeCfg.setMaxColumnsCount(conf.getRestrictConf().getMaxColumnsCount());
    writeCfg.setMaxPKColumnSize(conf.getRestrictConf().getPrimaryKeyColumnSize());
    ExecutorService service = new ThreadPoolExecutor(3, 3, 0L,
        TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    WriterCallback callback = new WriterCallback(collector);
    otsWriter = new DefaultOTSWriter(otsAsync, conf.getTableName(), writeCfg, callback, service);

    int expectColCnt = conf.getPrimaryKeyColumn().size() + conf.getAttributeColumn().size();
    Record rcd;
    while ((rcd = recordReceiver.getFromReader()) != null) {
      LOG.debug("Record Raw: {}", rcd.toString());

      int colCnt = rcd.getColumnNumber();
      if (colCnt != expectColCnt) {
        // 如果Column的个数和预期的个数不一致时，认为是系统故障或者用户配置Column错误，异常退出
        String exceptMsg = String.format(RECORD_AND_COLUMN_SIZE_ERROR, colCnt, expectColCnt);
        throw new IllegalArgumentException(exceptMsg);
      }

      // 类型转换
      try {
        List<OTSPKColumn> pkCols = conf.getPrimaryKeyColumn();
        List<OTSAttrColumn> cols = conf.getAttributeColumn();
        RowPrimaryKey pk = Common.getPKFromRecord(pkCols, rcd);
        List<Pair<String, ColumnValue>> attrs = Common.getAttrFromRecord(pkCols.size(), cols, rcd);

        String tblName = conf.getTableName();
        OTSOpType otsOpType = conf.getOperation();
        RowChange rowChange = Common.columnValuesToRowChange(tblName, otsOpType, pk, attrs);
        WithRecord withRecord = (WithRecord) rowChange;
        withRecord.setRecord(rcd);
        otsWriter.addRowChange(rowChange);
      } catch (IllegalArgumentException e) {
        LOG.warn("Found dirty data.", e);
        collector.collectDirtyRecord(rcd, e.getMessage());
      } catch (ClientException e) {
        LOG.warn("Found dirty data.", e);
        collector.collectDirtyRecord(rcd, e.getMessage());
      }
    }

    otsWriter.close();
    LOG.info("Writer slave finished.");
  }
}
