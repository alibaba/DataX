package com.alibaba.datax.plugin.writer.milvuswriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.fastjson2.JSONArray;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import io.milvus.v2.common.DataType;
import static io.milvus.v2.common.DataType.*;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MilvusSinkConverter {
    public JsonObject convertByType(JSONArray milvusColumnMeta, Record record) {
        JsonObject data = new JsonObject();
        Gson gson = new Gson();
        for(int i = 0; i < record.getColumnNumber(); i++) {
            String fieldType = milvusColumnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_TYPE);
            String fieldName = milvusColumnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME);
            Object rawData = record.getColumn(i).getRawData();
            Object field = convertToMilvusField(fieldType, rawData);
            data.add(fieldName, gson.toJsonTree(field));
        }
        return data;
    }

    private Object convertToMilvusField(String type, Object rawData) {
        Gson gson = new Gson();
        switch (valueOf(type)) {
            case Int8:
                return Byte.parseByte(rawData.toString());
            case Int16:
                return Short.parseShort(rawData.toString());
            case Int32:
                return Integer.parseInt(rawData.toString());
            case Int64:
                return Long.parseLong(rawData.toString());
            case Float:
                return java.lang.Float.parseFloat(rawData.toString());
            case String:
            case VarChar:
                return rawData.toString();
            case Bool:
                return Boolean.parseBoolean(rawData.toString());
            case JSON:
                return gson.fromJson(rawData.toString(), JsonObject.class);
            case Array:
                Type listType = new TypeToken<List<Object>>() {}.getType();
                return gson.fromJson(rawData.toString(), listType);
            case FloatVector:
                java.lang.Float[] floats = Arrays.stream(processVectorString(rawData)).map(java.lang.Float::parseFloat).toArray(java.lang.Float[]::new);
                return Arrays.stream(floats).collect(Collectors.toList());
            case BinaryVector:
                java.lang.Integer[] binarys = Arrays.stream(processVectorString(rawData)).map(java.lang.Integer::parseInt).toArray(java.lang.Integer[]::new);
                return BufferUtils.toByteBuffer(binarys);
            case Float16Vector:
            case BFloat16Vector:
                // all these data is byte format in milvus
                ByteBuffer binaryVector = (ByteBuffer) rawData;
                return gson.toJsonTree(binaryVector.array());
            case SparseFloatVector:
                return JsonParser.parseString(gson.toJson(rawData)).getAsJsonObject();
            default:
                throw new RuntimeException("Unsupported data type: " + type);
        }
    }

    private String[] processArrayString(Object rawData) {
        // Step 1: Remove square brackets
        String cleanedInput = rawData.toString().replace("[", "").replace("]", "");

        // Step 2: Split the string into an array of string numbers
        return cleanedInput.split(",\\s*");
    }

    private String[] processVectorString(Object rawData) {
        // Step 1: Remove square brackets
        String cleanedInput = rawData.toString().replace("[", "").replace("]", "");

        // Step 2: Split the string into an array of string numbers
        return cleanedInput.split(",\\s*");
    }

    public CreateCollectionReq.CollectionSchema prepareCollectionSchema(JSONArray milvusColumnMeta) {
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder().build();
        for (int i = 0; i < milvusColumnMeta.size(); i++) {
            AddFieldReq addFieldReq = AddFieldReq.builder()
                    .fieldName(milvusColumnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME))
                    .dataType(valueOf(milvusColumnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_TYPE)))
                    .build();
            if(milvusColumnMeta.getJSONObject(i).containsKey(KeyConstant.IS_PRIMARY_KEY)) {
                addFieldReq.setIsPrimaryKey(milvusColumnMeta.getJSONObject(i).getBoolean(KeyConstant.IS_PRIMARY_KEY));
            }
            if(milvusColumnMeta.getJSONObject(i).containsKey(KeyConstant.VECTOR_DIMENSION)) {
                addFieldReq.setDimension(milvusColumnMeta.getJSONObject(i).getInteger(KeyConstant.VECTOR_DIMENSION));
            }
            if(milvusColumnMeta.getJSONObject(i).containsKey(KeyConstant.IS_PARTITION_KEY)) {
                addFieldReq.setIsPartitionKey(milvusColumnMeta.getJSONObject(i).getBoolean(KeyConstant.IS_PARTITION_KEY));
            }
            if(milvusColumnMeta.getJSONObject(i).containsKey(KeyConstant.MAX_LENGTH)) {
                addFieldReq.setMaxLength(milvusColumnMeta.getJSONObject(i).getInteger(KeyConstant.MAX_LENGTH));
            }
            if(milvusColumnMeta.getJSONObject(i).containsKey(KeyConstant.ELEMENT_TYPE)) {
                addFieldReq.setElementType(DataType.valueOf(milvusColumnMeta.getJSONObject(i).getString(KeyConstant.ELEMENT_TYPE)));
                addFieldReq.setMaxLength(milvusColumnMeta.getJSONObject(i).getInteger(KeyConstant.MAX_LENGTH));
            }
            if(milvusColumnMeta.getJSONObject(i).containsKey(KeyConstant.MAX_CAPACITY)) {
                addFieldReq.setMaxCapacity(milvusColumnMeta.getJSONObject(i).getInteger(KeyConstant.MAX_CAPACITY));
            }
            collectionSchema.addField(addFieldReq);
        }
        return collectionSchema;
    }
}
