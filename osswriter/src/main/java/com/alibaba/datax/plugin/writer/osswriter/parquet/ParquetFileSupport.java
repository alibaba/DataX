package com.alibaba.datax.plugin.writer.osswriter.parquet;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.plugin.unstructuredstorage.writer.Key;
import com.alibaba.datax.plugin.writer.osswriter.Constant;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.hadoop.api.WriteSupport;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;

/**
 * @Author: guxuan
 * @Date 2022-05-17 16:25
 */
public class ParquetFileSupport extends WriteSupport<Record> {
    public static final Logger LOGGER = LoggerFactory.getLogger(ParquetFileSupport.class);
    private MessageType schema;
    private RecordConsumer recordConsumer;
    private boolean printStackTrace = true;

    // 不通类型的nullFormat
    private String nullFormat;

    private String dateFormat;
    private DateFormat dateParse;
    private Binary binaryForNull;
    private TaskPluginCollector taskPluginCollector;

    public ParquetFileSupport(MessageType schema, com.alibaba.datax.common.util.Configuration taskConfig, TaskPluginCollector taskPluginCollector) {
        this.schema = schema;
        // 不通类型的nullFormat
        this.nullFormat = taskConfig.getString(Key.NULL_FORMAT, Constant.DEFAULT_NULL_FORMAT);
        this.binaryForNull = Binary.fromString(this.nullFormat);

        this.dateFormat = taskConfig.getString(Key.DATE_FORMAT, null);
        if (StringUtils.isNotBlank(this.dateFormat)) {
            this.dateParse = new SimpleDateFormat(dateFormat);
        }

        this.taskPluginCollector = taskPluginCollector;
    }

    @Override
    public WriteContext init(Configuration configuration) {
        return new WriteContext(schema, new HashMap<String, String>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(Record values) {
        LOGGER.info("Writing parquet data using fields mode(The correct mode.)");
        List<Type> types = this.schema.getFields();
        if (values != null && types != null && values.getColumnNumber() == types.size()) {
            recordConsumer.startMessage();
            writeFields(types, values);
            recordConsumer.endMessage();
        }
    }

    private void writeFields(List<Type> types, Record values) {
        for (int i = 0; i < types.size(); i++) {
            Type type = types.get(i);
            Column value = values.getColumn(i);
            if (value != null) {
                try {
                    if (type.isPrimitive()) {
                        writePrimitiveType(type, value, i);
                    } else {
                        writeGroupType(type, (JSON) JSON.parse(value.asString()), i);
                    }
                } catch (Exception e) {
                    if (printStackTrace) {
                        printStackTrace = false;
                        LOGGER.warn("write to parquet error: {}", e.getMessage(), e);
                    }
                    // dirty data
                    if (null != this.taskPluginCollector) {
                        // job post 里面的merge taskPluginCollector 为null
                        this.taskPluginCollector.collectDirtyRecord(values, e, e.getMessage());
                    }
                }
            }
        }
    }

    private void writeFields(List<Type> types, JSONObject values) {
        for (int i = 0; i < types.size(); i++) {
            Type type = types.get(i);
            Object value = values.get(type.getName());

            if (value != null) {
                try {
                    if (type.isPrimitive()) {
                        writePrimitiveType(type, value, i);
                    } else {
                        writeGroupType(type, (JSON) value, i);
                    }
                } catch (Exception e) {
                    if (printStackTrace) {
                        printStackTrace = false;
                        LOGGER.warn("write to parquet error: {}", e.getMessage(), e);
                    }
                }
            } else {
                recordConsumer.addBinary(this.binaryForNull);
            }
        }
    }

    private void writeGroupType(Type type, JSON value, int index) {
        GroupType groupType = type.asGroupType();
        OriginalType originalType = groupType.getOriginalType();
        if (originalType != null) {
            switch (originalType) {
                case MAP:
                    writeMap(groupType, value, index);
                    break;
                case LIST:
                    writeList(groupType, value, index);
                    break;
                default:
                    break;
            }
        } else {
            // struct
            writeStruct(groupType, value, index);
        }
    }

    private void writeMap(GroupType groupType, JSON value, int index) {
        if (value == null) {
            return;
        }

        JSONObject json = (JSONObject) value;

        if (json.isEmpty()) {
            return;
        }

        recordConsumer.startField(groupType.getName(), index);

        recordConsumer.startGroup();

        // map
        // key_value start
        recordConsumer.startField("key_value", 0);
        recordConsumer.startGroup();

        List<Type> keyValueFields = groupType.getFields().get(0).asGroupType().getFields();
        Type keyType = keyValueFields.get(0);
        Type valueType = keyValueFields.get(1);
        for (String key : json.keySet()) {
            // key
            writePrimitiveType(keyType, key, 0);

            // value
            if (valueType.isPrimitive()) {
                writePrimitiveType(valueType, json.get(key), 1);
            } else {
                writeGroupType(valueType, (JSON) json.get(key), 1);
            }
        }

        recordConsumer.endGroup();
        recordConsumer.endField("key_value", 0);
        // key_value end

        recordConsumer.endGroup();
        recordConsumer.endField(groupType.getName(), index);
    }

    private void writeList(GroupType groupType, JSON value, int index) {
        if (value == null) {
            return;
        }

        JSONArray json = (JSONArray) value;

        if (json.isEmpty()) {
            return;
        }

        recordConsumer.startField(groupType.getName(), index);
        // list
        recordConsumer.startGroup();


        // list start
        recordConsumer.startField("list", 0);
        recordConsumer.startGroup();

        Type elementType = groupType.getFields().get(0).asGroupType().getFields().get(0);

        if (elementType.isPrimitive()) {
            for (Object elementValue : json) {
                writePrimitiveType(elementType, elementValue, 0);
            }
        } else {
            for (Object elementValue : json) {
                writeGroupType(elementType, (JSON) elementValue, 0);
            }
        }

        recordConsumer.endGroup();
        recordConsumer.endField("list", 0);
        // list end
        recordConsumer.endGroup();

        recordConsumer.endField(groupType.getName(), index);
    }

    private void writeStruct(GroupType groupType, JSON value, int index) {
        if (value == null) {
            return;
        }
        JSONObject json = (JSONObject) value;
        if (json.isEmpty()) {
            return;
        }

        recordConsumer.startField(groupType.getName(), index);
        // struct start
        recordConsumer.startGroup();

        writeFields(groupType.getFields(), json);
        recordConsumer.endGroup();
        // struct end
        recordConsumer.endField(groupType.getName(), index);
    }

    private void writePrimitiveType(Type type, Object value, int index) {
        if (value == null) {
            return;
        }

        recordConsumer.startField(type.getName(), index);
        PrimitiveType primitiveType = type.asPrimitiveType();

        switch (primitiveType.getPrimitiveTypeName()) {
            case BOOLEAN:
                recordConsumer.addBoolean((Boolean) value);
                break;
            case FLOAT:
                if (value instanceof Float) {
                    recordConsumer.addFloat(((Float) value).floatValue());
                } else if (value instanceof Double) {
                    recordConsumer.addFloat(((Double) value).floatValue());
                } else if (value instanceof Long) {
                    recordConsumer.addFloat(((Long) value).floatValue());
                } else if (value instanceof Integer) {
                    recordConsumer.addFloat(((Integer) value).floatValue());
                }
                break;
            case DOUBLE:
                if (value instanceof Float) {
                    recordConsumer.addDouble(((Float) value).doubleValue());
                } else if (value instanceof Double) {
                    recordConsumer.addDouble(((Double) value).doubleValue());
                } else if (value instanceof Long) {
                    recordConsumer.addDouble(((Long) value).doubleValue());
                } else if (value instanceof Integer) {
                    recordConsumer.addDouble(((Integer) value).doubleValue());
                }
                break;
            case INT32:
                if (value instanceof Integer) {
                    recordConsumer.addInteger((Integer) value);
                } else if (value instanceof Long) {
                    recordConsumer.addInteger(((Long) value).intValue());
                } else {
                    new IllegalArgumentException(
                            String.format("Invalid value: %s(clazz: %s) for field: %s", value, value.getClass(), type.getName())
                    );
                }
                break;
            case INT64:
            case INT96:
                if (value instanceof Integer) {
                    recordConsumer.addLong(((Integer) value).longValue());
                } else if (value instanceof Long) {
                    recordConsumer.addInteger(((Long) value).intValue());
                } else {
                    new IllegalArgumentException(
                            String.format("Invalid value: %s(clazz: %s) for field: %s", value, value.getClass(), type.getName())
                    );
                }
                break;
            case BINARY:
            default:
                recordConsumer.addBinary(Binary.fromString((String) value));
                break;
        }
        recordConsumer.endField(type.getName(), index);
    }

    private void writePrimitiveType(Type type, Column value, int index) {
        if (value == null || value.getRawData() == null) {
            return;
        }

        recordConsumer.startField(type.getName(), index);
        PrimitiveType primitiveType = type.asPrimitiveType();
        switch (primitiveType.getPrimitiveTypeName()) {
            case BOOLEAN:
                recordConsumer.addBoolean(value.asBoolean());
                break;
            case FLOAT:
                recordConsumer.addFloat(value.asDouble().floatValue());
                break;
            case DOUBLE:
                recordConsumer.addDouble(value.asDouble());
                break;
            case INT32:
                recordConsumer.addInteger(value.asLong().intValue());
                break;
            case INT64:
            case INT96:
                recordConsumer.addLong(value.asLong());
                break;
            case BINARY:
                String valueAsString2Write = null;
                if (Column.Type.DATE == value.getType() && null != this.dateParse) {
                    valueAsString2Write = dateParse.format(value.asDate());
                }
                else {
                    valueAsString2Write = value.asString();
                }
                recordConsumer.addBinary(Binary.fromString(valueAsString2Write));
                break;
            default:
                recordConsumer.addBinary(Binary.fromString(value.asString()));
                break;
        }
        recordConsumer.endField(type.getName(), index);
    }
}
