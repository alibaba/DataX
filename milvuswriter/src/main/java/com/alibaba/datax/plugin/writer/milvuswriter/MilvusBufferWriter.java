package com.alibaba.datax.plugin.writer.milvuswriter;

import com.google.gson.JsonObject;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.vector.request.UpsertReq;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class MilvusBufferWriter {

    private final MilvusClientV2 milvusClientV2;
    private final String collection;
    private final String partition;
    private final Integer batchSize;
    private List<JsonObject> dataCache;

    public MilvusBufferWriter(MilvusClientV2 milvusClientV2, String collection, String partition, Integer batchSize){
        this.milvusClientV2 = milvusClientV2;
        this.collection = collection;
        this.partition = partition;
        this.batchSize = batchSize;
        this.dataCache = new ArrayList<>();
    }
    public void write(JsonObject data){
        dataCache.add(data);
    }
    public Boolean needCommit(){
        return dataCache.size() >= batchSize;
    }
    public void commit(){
        if(dataCache.isEmpty()){
            log.info("dataCache is empty, skip commit");
            return;
        }
        UpsertReq upsertReq = UpsertReq.builder()
                .collectionName(collection)
                .data(dataCache)
                .build();
        if(partition != null){
            upsertReq.setPartitionName(partition);
        }
        milvusClientV2.upsert(upsertReq);
        dataCache = new ArrayList<>();
    }
}
