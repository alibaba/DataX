package com.alibaba.datax.plugin.reader.otsstreamreader.internal;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConstants;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.core.CheckpointTimeTracker;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.OTSStreamJobShard;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.StreamJob;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.GsonParser;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.OTSHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.OTSStreamJobShardUtil;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.StreamShard;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;

import static com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConstants.*;

public class OTSStreamReader {

    public static class Job extends Reader.Job {

        private OTSStreamReaderMasterProxy proxy = new OTSStreamReaderMasterProxy();
        @Override
        public List<Configuration> split(int adviceNumber) {
            return proxy.split(adviceNumber);
        }

        public void init() {
            try {
                OTSStreamReaderConfig config = OTSStreamReaderConfig.load(getPluginJobConf());
                proxy.init(config);
            } catch (TableStoreException ex) {
                throw DataXException.asDataXException(new OTSReaderError(ex.getErrorCode(), "OTS ERROR"), ex.toString(), ex);
            } catch (Exception ex) {
                throw DataXException.asDataXException(OTSReaderError.ERROR, ex.toString(), ex);
            }
        }

        public void destroy() {
            this.proxy.close();
        }
    }

    public static class Task extends Reader.Task {

        private OTSStreamReaderSlaveProxy proxy = new OTSStreamReaderSlaveProxy();

        @Override
        public void init() {
            try {
                OTSStreamReaderConfig config = GsonParser.jsonToConfig(
                        (String) this.getPluginJobConf().get(OTSStreamReaderConstants.CONF));
                List<String> ownedShards = GsonParser.jsonToList(
                    (String) this.getPluginJobConf().get(OTSStreamReaderConstants.OWNED_SHARDS));

                boolean confSimplifyEnable = this.getPluginJobConf().getBool(CONF_SIMPLIFY_ENABLE,
                    DEFAULT_CONF_SIMPLIFY_ENABLE_VALUE);

                StreamJob streamJob;
                List<StreamShard> allShards;

                if (confSimplifyEnable) {
                    //不要从conf里获取, 避免分布式模式下Job Split切分出来的Config膨胀过大
                    String version = this.getPluginJobConf().getString(OTSStreamReaderConstants.VERSION);
                    OTSStreamJobShard otsStreamJobShard = OTSStreamJobShardUtil.getOTSStreamJobShard(config, version);

                    streamJob = otsStreamJobShard.getStreamJob();
                    allShards = otsStreamJobShard.getAllShards();

                } else {
                    streamJob = StreamJob.fromJson(
                            (String) this.getPluginJobConf().get(OTSStreamReaderConstants.STREAM_JOB));
                    allShards = GsonParser.fromJson(
                            (String) this.getPluginJobConf().get(OTSStreamReaderConstants.ALL_SHARDS));
                }

                proxy.init(config, streamJob, allShards, new HashSet<String>(ownedShards));
            } catch (TableStoreException ex) {
                throw DataXException.asDataXException(new OTSReaderError(ex.getErrorCode(), "OTS ERROR"), ex.toString(), ex);
            } catch (Exception ex) {
                throw DataXException.asDataXException(OTSReaderError.ERROR, ex.toString(), ex);
            }
        }

        @Override
        public void startRead(RecordSender recordSender) {
            proxy.startRead(recordSender);
        }

        public void destroy() {
            proxy.close();
        }
    }
}
