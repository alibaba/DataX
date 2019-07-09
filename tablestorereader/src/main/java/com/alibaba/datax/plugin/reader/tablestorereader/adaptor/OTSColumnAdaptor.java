package com.alibaba.datax.plugin.reader.tablestorereader.adaptor;

import com.alibaba.datax.plugin.reader.tablestorereader.model.TableStoreColumn;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.google.gson.*;
import org.apache.commons.codec.binary.Base64;

import java.lang.reflect.Type;

public class OTSColumnAdaptor implements JsonDeserializer<TableStoreColumn>, JsonSerializer<TableStoreColumn>{
    private final static String NAME = "name";
    private final static String COLUMN_TYPE = "column_type";
    private final static String VALUE_TYPE = "value_type";
    private final static String VALUE = "value";

    private void serializeConstColumn(JsonObject json, TableStoreColumn obj) {
        switch (obj.getValueType()) {
      case STRING : 
          json.add(VALUE_TYPE, new JsonPrimitive(ColumnType.STRING.toString()));
          json.add(VALUE, new JsonPrimitive(obj.getValue().asString()));
          break;
      case INTEGER : 
          json.add(VALUE_TYPE, new JsonPrimitive(ColumnType.INTEGER.toString()));
          json.add(VALUE, new JsonPrimitive(obj.getValue().asLong()));
          break;
      case DOUBLE : 
          json.add(VALUE_TYPE, new JsonPrimitive(ColumnType.DOUBLE.toString())); 
          json.add(VALUE, new JsonPrimitive(obj.getValue().asDouble()));
          break;
      case BOOLEAN : 
          json.add(VALUE_TYPE, new JsonPrimitive(ColumnType.BOOLEAN.toString())); 
          json.add(VALUE, new JsonPrimitive(obj.getValue().asBoolean()));
          break;
      case BINARY : 
          json.add(VALUE_TYPE, new JsonPrimitive(ColumnType.BINARY.toString())); 
          json.add(VALUE, new JsonPrimitive(Base64.encodeBase64String(obj.getValue().asBytes())));
          break;
      default:
          throw new IllegalArgumentException("Unsupport serialize the type : " + obj.getValueType() + "");
        }
    }
    
    private TableStoreColumn deserializeConstColumn(JsonObject obj) {
        String strType = obj.getAsJsonPrimitive(VALUE_TYPE).getAsString();
        ColumnType type = ColumnType.valueOf(strType);
        
        JsonPrimitive jsonValue =  obj.getAsJsonPrimitive(VALUE);

        switch (type) {
        case STRING : 
            return TableStoreColumn.fromConstStringColumn(jsonValue.getAsString());
        case INTEGER : 
            return TableStoreColumn.fromConstIntegerColumn(jsonValue.getAsLong());
        case DOUBLE : 
            return TableStoreColumn.fromConstDoubleColumn(jsonValue.getAsDouble());
        case BOOLEAN : 
            return TableStoreColumn.fromConstBoolColumn(jsonValue.getAsBoolean());
        case BINARY : 
            return TableStoreColumn.fromConstBytesColumn(Base64.decodeBase64(jsonValue.getAsString()));
        default:
            throw new IllegalArgumentException("Unsupport deserialize the type : " + type + "");
        }
    }

    private void serializeNormalColumn(JsonObject json, TableStoreColumn obj) {
        json.add(NAME, new JsonPrimitive(obj.getName()));
    }

    private TableStoreColumn deserializeNormarlColumn(JsonObject obj) {
        return TableStoreColumn.fromNormalColumn(obj.getAsJsonPrimitive(NAME).getAsString());
    }
    
    @Override
    public JsonElement serialize(TableStoreColumn obj, Type t,
                                 JsonSerializationContext c) {
        JsonObject json = new JsonObject();

        switch (obj.getColumnType()) {
        case CONST:
            json.add(COLUMN_TYPE, new JsonPrimitive(TableStoreColumn.OTSColumnType.CONST.toString()));
            serializeConstColumn(json, obj);
            break;
        case NORMAL:
            json.add(COLUMN_TYPE, new JsonPrimitive(TableStoreColumn.OTSColumnType.NORMAL.toString()));
            serializeNormalColumn(json, obj);
            break;
        default:
            throw new IllegalArgumentException("Unsupport serialize the type : " + obj.getColumnType() + ""); 
        }
        return json;
    }

    @Override
    public TableStoreColumn deserialize(JsonElement ele, Type t,
                                        JsonDeserializationContext c) throws JsonParseException {
        JsonObject obj = ele.getAsJsonObject();
        String strColumnType = obj.getAsJsonPrimitive(COLUMN_TYPE).getAsString();
        TableStoreColumn.OTSColumnType columnType = TableStoreColumn.OTSColumnType.valueOf(strColumnType);

        switch(columnType) {
        case CONST:
            return deserializeConstColumn(obj);
        case NORMAL:
            return deserializeNormarlColumn(obj);
        default:
            throw new IllegalArgumentException("Unsupport deserialize the type : " + columnType + "");
        }
    }
}
