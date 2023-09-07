package com.alibaba.datax.plugin.reader.otsstreamreader.internal;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConstants;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.core.CheckpointTimeTracker;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.core.OTSStreamReaderChecker;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.StreamJob;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.GsonParser;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.OTSHelper;
import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConstants.CONF_SIMPLIFY_ENABLE;

public class OTSStreamReaderMasterProxy {

    private OTSStreamReaderConfig conf = null;
    private SyncClientInterface ots = null;

    private StreamJob streamJob;
    private List<StreamShard> allShards;
    private String version;

    private static final Logger LOG = LoggerFactory.getLogger(OTSStreamReaderConfig.class);

    public void init(OTSStreamReaderConfig config) throws Exception {
        this.conf = config;

        // Init ots
        ots = OTSHelper.getOTSInstance(conf);

        // 创建Checker
        OTSStreamReaderChecker checker = new OTSStreamReaderChecker(ots, conf);

        // 检查Stream是否开启，选取的时间范围是否可以导出。
        checker.checkStreamEnabledAndTimeRangeOK();

        // 检查StatusTable是否存在，若不存在则创建StatusTable。
        checker.checkAndCreateStatusTableIfNotExist();

        // 删除StatusTable记录的对应EndTime时刻的Checkpoint信息。防止本次任务受到之前导出任务的影响。
        String streamId = OTSHelper.getStreamResponse(ots, config.getDataTable(), config.isTimeseriesTable()).getStreamId();
        CheckpointTimeTracker checkpointInfoTracker = new CheckpointTimeTracker(ots, config.getStatusTable(), streamId);
        checkpointInfoTracker.clearAllCheckpoints(config.getEndTimestampMillis());

        SyncClientInterface ots = OTSHelper.getOTSInstance(config);

        allShards = OTSHelper.getOrderedShardList(ots, streamId, conf.isTimeseriesTable());
        List<String> shardIds = new ArrayList<String>();
        for (StreamShard shard : allShards) {
            shardIds.add(shard.getShardId());
        }

        this.version = "" + System.currentTimeMillis() + "-" + UUID.randomUUID();
        LOG.info("version is: {}", this.version);

        streamJob = new StreamJob(conf.getDataTable(), streamId, version, new HashSet<String>(shardIds),
                conf.getStartTimestampMillis(), conf.getEndTimestampMillis());
        checkpointInfoTracker.writeStreamJob(streamJob);

        LOG.info("Start stream job: {}.", streamJob.toJson());
    }

    /**
     * For testing purpose.
     *
     * @param streamJob
     */
    void setStreamJob(StreamJob streamJob) {
        this.streamJob = streamJob;
    }

    public StreamJob getStreamJob() {
        return streamJob;
    }

    public List<Configuration> split(int adviceNumber) {
        int shardCount = streamJob.getShardIds().size();
        int splitNumber = Math.min(adviceNumber, shardCount);
        int splitSize = shardCount / splitNumber;
        List<Configuration> configurations = new ArrayList<Configuration>();

        List<String> shardIds = new ArrayList<String>(streamJob.getShardIds());
        Collections.shuffle(shardIds);
        int start = 0;
        int end = 0;
        int remain = shardCount % splitNumber;
        for (int i = 0; i < splitNumber; i++) {
            start = end;
            end = start + splitSize;

            if (remain > 0) {
                end += 1;
                remain -= 1;
            }

            Configuration configuration = Configuration.newDefault();
            configuration.set(OTSStreamReaderConstants.CONF, GsonParser.configToJson(conf));

            // Fix #39430646 [离线同步分布式]DataX OTSStreamReader插件分布式模式优化瘦身
            if (conf.isConfSimplifyEnable()) {
                configuration.set(OTSStreamReaderConstants.VERSION, this.version);
                configuration.set(CONF_SIMPLIFY_ENABLE, true);
            } else {
                configuration.set(OTSStreamReaderConstants.STREAM_JOB, streamJob.toJson());
                configuration.set(OTSStreamReaderConstants.ALL_SHARDS, GsonParser.toJson(allShards));
            }

            configuration.set(OTSStreamReaderConstants.OWNED_SHARDS, GsonParser.listToJson(shardIds.subList(start, end)));
            configurations.add(configuration);
        }
        LOG.info("Master split to {} slave, with advice number {}.", configurations.size(), adviceNumber);
        return configurations;
    }

    public void close(){
        ots.shutdown();
    }
}
