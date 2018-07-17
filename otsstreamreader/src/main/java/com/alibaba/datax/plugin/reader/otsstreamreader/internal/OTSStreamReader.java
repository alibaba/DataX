package com.alibaba.datax.plugin.reader.otsstreamreader.internal;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConstants;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.StreamJob;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.GsonParser;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.StreamShard;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

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
        public void startRead(RecordSender recordSender) {
            proxy.startRead(recordSender);
        }

        public void init() {
            try {
                OTSStreamReaderConfig config = GsonParser.jsonToConfig(
                        (String) this.getPluginJobConf().get(OTSStreamReaderConstants.CONF));
                StreamJob streamJob = StreamJob.fromJson(
                        (String) this.getPluginJobConf().get(OTSStreamReaderConstants.STREAM_JOB));
                List<String> ownedShards = GsonParser.jsonToList(
                        (String) this.getPluginJobConf().get(OTSStreamReaderConstants.OWNED_SHARDS));
                List<StreamShard> allShards = GsonParser.fromJson(
                        (String) this.getPluginJobConf().get(OTSStreamReaderConstants.ALL_SHARDS));
                proxy.init(config, streamJob, allShards, new HashSet<String>(ownedShards));
            } catch (TableStoreException ex) {
                throw DataXException.asDataXException(new OTSReaderError(ex.getErrorCode(), "OTS ERROR"), ex.toString(), ex);
            } catch (Exception ex) {
                throw DataXException.asDataXException(OTSReaderError.ERROR, ex.toString(), ex);
            }
        }

        public void destroy() {
            proxy.close();
        }
    }
}
