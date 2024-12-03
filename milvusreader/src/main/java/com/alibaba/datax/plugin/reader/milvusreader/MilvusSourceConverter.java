package com.alibaba.datax.plugin.reader.milvusreader;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.google.gson.Gson;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MilvusSourceConverter {

    public Record toDataXRecord(Record record, QueryResultsWrapper.RowRecord rowRecord, CreateCollectionReq.CollectionSchema collectionSchema) {
        Map<String, Object> fields =  rowRecord.getFieldValues();

        for (int i = 0; i < collectionSchema.getFieldSchemaList().size(); i++) {
            CreateCollectionReq.FieldSchema fieldSchema = collectionSchema.getFieldSchemaList().get(i);
            String fieldName = fieldSchema.getName();
            Object fieldValue = fields.get(fieldName);
            Column column = convertToDataXColumn(fieldSchema.getDataType(), fieldValue);
            record.addColumn(column);
        }
        return record;
    }

    private Column convertToDataXColumn(DataType dataType, Object fieldValue) {
        Gson gson = new Gson();
        switch (dataType) {
            case Bool:
                return new BoolColumn(Boolean.getBoolean(fieldValue.toString()));
            case Int8:
            case Int16:
            case Int32:
            case Int64:
                return new LongColumn(Integer.parseInt(fieldValue.toString()));
            case Float:
            case Double:
                return new DoubleColumn(java.lang.Double.parseDouble(fieldValue.toString()));
            case VarChar:
            case String:
                return new StringColumn(fieldValue.toString());
            case JSON:
                return new StringColumn(gson.toJson(fieldValue));
            case Array:
                return new StringColumn(gson.toJson(fieldValue));
            case FloatVector:
                List<Float> floats = (List<Float>) fieldValue;
                return new StringColumn(Arrays.toString(floats.toArray()));
            case BinaryVector:
                Integer[] binarys = BufferUtils.toIntArray((ByteBuffer) fieldValue);
                return new StringColumn(Arrays.toString(binarys));
            default:
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }
}
