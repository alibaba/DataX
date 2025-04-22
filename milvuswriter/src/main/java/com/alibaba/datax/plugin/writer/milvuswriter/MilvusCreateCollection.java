package com.alibaba.datax.plugin.writer.milvuswriter;

import java.util.List;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.milvuswriter.enums.SchemaCreateModeEnum;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;

import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import lombok.extern.slf4j.Slf4j;

import static io.milvus.v2.common.DataType.valueOf;

/**
 * @author ziming(子茗)
 * @date 12/27/24
 * @description
 */
@Slf4j
public class MilvusCreateCollection {

    private Configuration config;

    MilvusCreateCollection(Configuration originalConfig) {
        this.config = originalConfig;
    }

    public void createCollectionByMode(MilvusClient milvusClient) {
        String collection = this.config.getString(KeyConstant.COLLECTION);
        SchemaCreateModeEnum schemaCreateMode = SchemaCreateModeEnum.getEnum(this.config.getString(KeyConstant.SCHAME_CREATE_MODE));
        List<MilvusColumn> milvusColumnMeta = JSON.parseObject(config.getString(KeyConstant.COLUMN), new TypeReference<List<MilvusColumn>>() {
        });
        Boolean hasCollection = milvusClient.hasCollection(collection);
        if (schemaCreateMode == SchemaCreateModeEnum.CREATEIFNOTEXIT) {
            // create collection
            if (hasCollection) {
                log.info("collection[{}] already exists, continue create", collection);
            } else {
                log.info("creating collection[{}]", collection);
                CreateCollectionReq.CollectionSchema collectionSchema = prepareCollectionSchema(milvusColumnMeta);
                milvusClient.createCollection(collection, collectionSchema);
            }
        } else if (schemaCreateMode == SchemaCreateModeEnum.RECREATE) {
            if (hasCollection) {
                log.info("collection  already exist, try to drop");
                milvusClient.dropCollection(collection);
            }
            log.info("creating collection[{}]", collection);
            CreateCollectionReq.CollectionSchema collectionSchema = prepareCollectionSchema(milvusColumnMeta);
            milvusClient.createCollection(collection, collectionSchema);
        } else if (schemaCreateMode == SchemaCreateModeEnum.IGNORE && !hasCollection) {
            log.error("Collection not exist, throw exception");
            throw new RuntimeException("Collection not exist");
        }
    }

    private CreateCollectionReq.CollectionSchema prepareCollectionSchema(List<MilvusColumn> milvusColumnMeta) {
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder().build();
        for (int i = 0; i < milvusColumnMeta.size(); i++) {
            MilvusColumn milvusColumn = milvusColumnMeta.get(i);
            AddFieldReq addFieldReq = AddFieldReq.builder()
                    .fieldName(milvusColumn.getName())
                    .dataType(valueOf(milvusColumn.getType()))
                    .build();
            if (milvusColumn.getPrimaryKey() != null) {
                addFieldReq.setIsPrimaryKey(milvusColumn.getPrimaryKey());
            }
            if (milvusColumn.getDimension() != null) {
                addFieldReq.setDimension(milvusColumn.getDimension());
            }
            if (milvusColumn.getPartitionKey() != null) {
                addFieldReq.setIsPartitionKey(milvusColumn.getPartitionKey());
            }
            if (milvusColumn.getMaxLength() != null) {
                addFieldReq.setMaxLength(milvusColumn.getMaxLength());
            }
            if (milvusColumn.getAutoId() != null) {
                addFieldReq.setAutoID(milvusColumn.getAutoId());
            }
            if (milvusColumn.getMaxCapacity() != null) {
                addFieldReq.setMaxCapacity(milvusColumn.getMaxCapacity());
            }
            if (milvusColumn.getElementType() != null) {
                addFieldReq.setElementType(DataType.valueOf(milvusColumn.getElementType()));
            }
            try {
                collectionSchema.addField(addFieldReq);
            } catch (Exception e) {
                log.error("add filed[{}] error", milvusColumn.getName());
                throw e;
            }
        }
        Boolean enableDynamic = config.getBool(KeyConstant.ENABLE_DYNAMIC_SCHEMA);
        if (enableDynamic != null) {
            collectionSchema.setEnableDynamicField(enableDynamic);
        }
        return collectionSchema;
    }
}
