package com.alibaba.datax.plugin.reader.otsstreamreader.internal.model;

import com.alicloud.openservices.tablestore.model.StreamShard;

import java.util.List;

/**
 * OTS streamJob & allShards model
 *
 * @author mingya.wmy (云时)
 */
public class OTSStreamJobShard {

    private StreamJob streamJob;

    private List<StreamShard> allShards;

    public OTSStreamJobShard() {
    }

    public OTSStreamJobShard(StreamJob streamJob, List<StreamShard> allShards) {
        this.streamJob = streamJob;
        this.allShards = allShards;
    }

    public StreamJob getStreamJob() {
        return streamJob;
    }

    public void setStreamJob(StreamJob streamJob) {
        this.streamJob = streamJob;
    }

    public List<StreamShard> getAllShards() {
        return allShards;
    }

    public void setAllShards(List<StreamShard> allShards) {
        this.allShards = allShards;
    }

}
