package com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.core.CheckpointTimeTracker;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.OTSStreamJobShard;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.StreamJob;
import com.alibaba.fastjson.JSON;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.StreamShard;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConstants.DEFAULT_SLEEP_TIME_IN_MILLS;
import static com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConstants.RETRY_TIMES;

/**
 * @author mingya.wmy (云时)
 */
public class OTSStreamJobShardUtil {

    private static OTSStreamJobShard otsStreamJobShard = null;

    /**
     * 获取全局OTS StreamJob 和 allShards ，懒汉单例模式，减少对OTS接口交互频次
     * 备注：config 和 version 所有TASK 均一样
     *
     * @param config
     * @param version
     * @return
     * @throws Exception
     */
    public static OTSStreamJobShard getOTSStreamJobShard(OTSStreamReaderConfig config, String version) throws Exception {
        if (otsStreamJobShard == null) {
            synchronized (OTSHelper.class) {
                if (otsStreamJobShard == null) {
                    otsStreamJobShard = RetryUtil.executeWithRetry(new Callable<OTSStreamJobShard>() {
                        @Override
                        public OTSStreamJobShard call() throws Exception {
                            return getOTSStreamJobShardByOtsClient(config, version);
                        }
                    }, RETRY_TIMES, DEFAULT_SLEEP_TIME_IN_MILLS, true);
                }
            }
        }

        return otsStreamJobShard;
    }

    /**
     * 获取OTS StreamJob 和 allShards
     *
     * @param config OTS CONF
     * @param version OTS STREAM VERSION
     * @return
     */
    private static OTSStreamJobShard getOTSStreamJobShardByOtsClient(OTSStreamReaderConfig config, String version) {
        // Init ots，Task阶段从OTS中获取 allShards 和 streamJob
        SyncClientInterface ots = null;
        try {
            ots = OTSHelper.getOTSInstance(config);
            String streamId = OTSHelper.getStreamResponse(ots, config.getDataTable(), config.isTimeseriesTable()).getStreamId();
            List<StreamShard> allShards = OTSHelper.getOrderedShardList(ots, streamId, config.isTimeseriesTable());

            CheckpointTimeTracker checkpointInfoTracker = new CheckpointTimeTracker(ots, config.getStatusTable(), streamId);
            StreamJob streamJobFromCPT = checkpointInfoTracker.readStreamJob(config.getEndTimestampMillis());
            if (!StringUtils.equals(streamJobFromCPT.getVersion(), version)) {
                throw new RuntimeException(String.format("streamJob version (\"%s\") is not equal to \"%s\", streamJob: %s",
                    streamJobFromCPT.getVersion(), version, JSON.toJSONString(streamJobFromCPT)));
            }

            Set<String> shardIdSetsFromTracker = streamJobFromCPT.getShardIds();

            if (shardIdSetsFromTracker == null || shardIdSetsFromTracker.isEmpty()) {
                throw new RuntimeException(String.format("StreamJob [statusTable=%s, streamId=%s] shardIds can't be null!",
                    config.getStatusTable(), streamId));
            }

            Set<String> currentAllStreamShardIdSets = allShards.stream().map(streamShard -> streamShard.getShardId()).collect(Collectors.toSet());

            for (String shardId: shardIdSetsFromTracker) {
                if (!currentAllStreamShardIdSets.contains(shardId)) {
                    allShards.add(new StreamShard(shardId));
                }
            }

            StreamJob streamJob = new StreamJob(config.getDataTable(), streamId, version, shardIdSetsFromTracker,
                config.getStartTimestampMillis(), config.getEndTimestampMillis());

            return new OTSStreamJobShard(streamJob, allShards);
        } catch (Throwable e) {
            throw new DataXException(String.format("Get ots shards error: %s", e.getMessage()));
        } finally {
            if (ots != null) {
                ots.shutdown();
            }
        }
    }

}
