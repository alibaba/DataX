package com.alibaba.datax.plugin.writer.milvuswriter;

import java.util.List;

import com.alibaba.datax.common.util.Configuration;

import com.google.gson.JsonObject;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.partition.request.HasPartitionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * @author ziming(子茗)
 * @date 12/27/24
 * @description
 */
@Slf4j
public class MilvusClient {
    private MilvusClientV2 milvusClientV2;

    public MilvusClient(Configuration conf) {
        // connect to milvus
        ConnectConfig connectConfig = ConnectConfig.builder().uri(conf.getString(KeyConstant.ENDPOINT)).build();
        String token = null;
        if (conf.getString(KeyConstant.TOKEN) != null) {
            token = conf.getString(KeyConstant.TOKEN);
        } else {
            token = conf.getString(KeyConstant.USERNAME) + ":" + conf.getString(KeyConstant.PASSWORD);
        }
        connectConfig.setToken(token);
        String database = conf.getString(KeyConstant.DATABASE);
        if (StringUtils.isNotBlank(database)) {
            log.info("use database {}", database);
            connectConfig.setDbName(conf.getString(KeyConstant.DATABASE));
        }
        Integer connectTimeOut = conf.getInt(KeyConstant.CONNECT_TIMEOUT_MS);
        if (connectTimeOut != null) {
            connectConfig.setConnectTimeoutMs(connectTimeOut);
        }
        this.milvusClientV2 = new MilvusClientV2(connectConfig);
    }

    public void upsert(String collection, String partition, List<JsonObject> data) {
        UpsertReq upsertReq = UpsertReq.builder().collectionName(collection).data(data).build();
        if (StringUtils.isNotEmpty(partition)) {
            upsertReq.setPartitionName(partition);
        }
        milvusClientV2.upsert(upsertReq);
    }

    public void insert(String collection, String partition, List<JsonObject> data) {
        InsertReq insertReq = InsertReq.builder().collectionName(collection).data(data).build();
        if (StringUtils.isNotEmpty(partition)) {
            insertReq.setPartitionName(partition);
        }
        milvusClientV2.insert(insertReq);
    }

    public Boolean hasCollection(String collection) {
        HasCollectionReq build = HasCollectionReq.builder().collectionName(collection).build();
        return milvusClientV2.hasCollection(build);
    }

    public void createCollection(String collection, CreateCollectionReq.CollectionSchema schema) {
        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder().collectionName(collection).collectionSchema(schema).build();
        milvusClientV2.createCollection(createCollectionReq);
    }

    public void dropCollection(String collection) {
        DropCollectionReq request = DropCollectionReq.builder().collectionName(collection).build();
        milvusClientV2.dropCollection(request);
    }
    public Boolean hasPartition(String collection, String partition) {
        HasPartitionReq hasPartitionReq = HasPartitionReq.builder().collectionName(collection).partitionName(partition).build();
        return milvusClientV2.hasPartition(hasPartitionReq);
    }

    public void createPartition(String collectionName, String partitionName) {
        CreatePartitionReq createPartitionReq = CreatePartitionReq.builder().collectionName(collectionName).partitionName(partitionName).build();
        milvusClientV2.createPartition(createPartitionReq);
    }

    public void close() {
        log.info("Closing Milvus client");
        milvusClientV2.close();
    }
}
