package com.alibaba.datax.plugin.writer.milvuswriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.plugin.writer.milvuswriter.enums.WriteModeEnum;
import com.alibaba.fastjson2.JSONArray;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.vector.request.data.BFloat16Vec;
import io.milvus.v2.service.vector.request.data.Float16Vec;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;

@Slf4j
public class MilvusBufferWriter {

    private final MilvusClient milvusClient;
    private final String collection;
    private final Integer batchSize;
    private List<JsonObject> dataCache;
    private List<MilvusColumn> milvusColumnMeta;
    private WriteModeEnum writeMode;
    private String partition;

    public MilvusBufferWriter(MilvusClient milvusClient, Configuration writerSliceConfig) {
        this.milvusClient = milvusClient;
        this.collection = writerSliceConfig.getString(KeyConstant.COLLECTION);
        this.batchSize = writerSliceConfig.getInt(KeyConstant.BATCH_SIZE, 100);
        this.dataCache = new ArrayList<>(batchSize);
        this.milvusColumnMeta = JSON.parseObject(writerSliceConfig.getString(KeyConstant.COLUMN), new TypeReference<List<MilvusColumn>>() {
        });
        this.writeMode = WriteModeEnum.getEnum(writerSliceConfig.getString(KeyConstant.WRITE_MODE));
        this.partition = writerSliceConfig.getString(KeyConstant.PARTITION);
    }

    public void add(Record record, TaskPluginCollector taskPluginCollector) {
        try {
            JsonObject data = this.convertByType(milvusColumnMeta, record);
            dataCache.add(data);
        } catch (Exception e) {
            taskPluginCollector.collectDirtyRecord(record, String.format("parse record error errorMessage: %s", e.getMessage()));
        }
    }

    public Boolean needCommit() {
        return dataCache.size() >= batchSize;
    }

    public void commit() {
        if (dataCache.isEmpty()) {
            log.info("dataCache is empty, skip commit");
            return;
        }
        if (writeMode == WriteModeEnum.INSERT) {
            milvusClient.insert(collection, partition, dataCache);
        } else {
            milvusClient.upsert(collection, partition, dataCache);
        }
        dataCache = new ArrayList<>(batchSize);
    }

    public int getDataCacheSize() {
        return dataCache.size();
    }

    private JsonObject convertByType(List<MilvusColumn> milvusColumnMeta, Record record) {
        JsonObject data = new JsonObject();
        Gson gson = new Gson();
        for (int i = 0; i < record.getColumnNumber(); i++) {
            MilvusColumn milvusColumn = milvusColumnMeta.get(i);
            DataType fieldType = milvusColumn.getMilvusTypeEnum();
            String fieldName = milvusColumn.getName();
            Column column = record.getColumn(i);
            try {
                Object field = convertToMilvusField(fieldType, column, milvusColumn);
                data.add(fieldName, gson.toJsonTree(field));
            } catch (Exception e) {
                log.error("parse error for column: {} errorMessage: {}", fieldName, e.getMessage(), e);
                throw e;
            }
        }
        return data;
    }

    //值需要跟这里匹配：io.milvus.param.ParamUtils#checkFieldData(io.milvus.param.collection.FieldType, java.util.List<?>, boolean)
    private Object convertToMilvusField(DataType type, Column column, MilvusColumn milvusColumn) {
        if (column.getRawData() == null) {
            return null;
        }
        switch (type) {
            case Int8:
            case Int16:
            case Int32:
            case Int64:
                return column.asLong();
            case Float:
            case Double:
                return column.asDouble();
            case String:
            case VarChar:
                return column.asString();
            case Bool:
                return column.asBoolean();
            case BFloat16Vector:
                JSONArray bFloat16ArrayJson = JSON.parseArray(column.asString());
                List<Float> bfloat16Vector = new ArrayList<>();
                for (int i = 0; i < bFloat16ArrayJson.size(); i++) {
                    Float value = Float.parseFloat(bFloat16ArrayJson.getString(i));
                    bfloat16Vector.add(value);
                }
                BFloat16Vec bFloat16Vec = new BFloat16Vec(bfloat16Vector);
                ByteBuffer byteBuffer = (ByteBuffer) bFloat16Vec.getData();
                return byteBuffer.array();
            case Float16Vector:
                JSONArray float16ArrayJson = JSON.parseArray(column.asString());
                List<Float> float16Vector = new ArrayList<>();
                for (int i = 0; i < float16ArrayJson.size(); i++) {
                    Float floatValue = Float.parseFloat(float16ArrayJson.getString(i));
                    float16Vector.add(floatValue);
                }
                Float16Vec float16Vec = new Float16Vec(float16Vector);
                ByteBuffer data = (ByteBuffer) float16Vec.getData();
                return data.array();
            case BinaryVector:
                return column.asBytes();
            case FloatVector:
                JSONArray arrayJson = JSON.parseArray(column.asString());
                return arrayJson.stream().map(item -> Float.parseFloat(String.valueOf(item))).collect(Collectors.toList());
            case SparseFloatVector:
                //[3:0.5, 24:0.8, 76:0.2]
                try {
                    JSONArray sparseFloatArray = JSON.parseArray(column.asString());
                    TreeMap<Long, Float> mapValue = new TreeMap<>();
                    for (int i = 0; i < sparseFloatArray.size(); i++) {
                        String value = sparseFloatArray.getString(i);
                        String[] split = value.split(":");
                        Long key = Long.parseLong(split[0]);
                        Float val = Float.parseFloat(split[1]);
                        mapValue.put(key, val);
                    }
                    return mapValue;
                } catch (Exception e) {
                    log.error("parse column[{}] SparseFloatVector value error, value should like [3:0.5, 24:0.8, 76:0.2], but get:{}", milvusColumn.getName(), column.asString());
                    throw e;
                }
            case JSON:
                return column.asString();
            case Array:
                JSONArray parseArray = JSON.parseArray(column.asString());
                return parseArray.stream().map(item -> String.valueOf(item)).collect(Collectors.toList());
            default:
                throw new RuntimeException(String.format("Unsupported data type[%s]", type));
        }
    }
}