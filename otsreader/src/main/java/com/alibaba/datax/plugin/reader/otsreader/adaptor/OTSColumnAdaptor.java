package com.alibaba.datax.plugin.reader.otsreader.adaptor;

import java.lang.reflect.Type;

import org.apache.commons.codec.binary.Base64;

import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.aliyun.openservices.ots.model.ColumnType;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class OTSColumnAdaptor implements JsonDeserializer<OTSColumn>, JsonSerializer<OTSColumn>{
    private final static String NAME = "name";
    private final static String COLUMN_TYPE = "column_type";
    private final static String VALUE_TYPE = "value_type";
    private final static String VALUE = "value";

    private void serializeConstColumn(JsonObject json, OTSColumn obj) {
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
    
    private OTSColumn deserializeConstColumn(JsonObject obj) {
        String strType = obj.getAsJsonPrimitive(VALUE_TYPE).getAsString();
        ColumnType type = ColumnType.valueOf(strType);
        
        JsonPrimitive jsonValue =  obj.getAsJsonPrimitive(VALUE);

        switch (type) {
        case STRING : 
            return OTSColumn.fromConstStringColumn(jsonValue.getAsString());
        case INTEGER : 
            return OTSColumn.fromConstIntegerColumn(jsonValue.getAsLong());
        case DOUBLE : 
            return OTSColumn.fromConstDoubleColumn(jsonValue.getAsDouble());
        case BOOLEAN : 
            return OTSColumn.fromConstBoolColumn(jsonValue.getAsBoolean());
        case BINARY : 
            return OTSColumn.fromConstBytesColumn(Base64.decodeBase64(jsonValue.getAsString()));
        default:
            throw new IllegalArgumentException("Unsupport deserialize the type : " + type + "");
        }
    }

    private void serializeNormalColumn(JsonObject json, OTSColumn obj) {
        json.add(NAME, new JsonPrimitive(obj.getName()));
    }

    private OTSColumn deserializeNormarlColumn(JsonObject obj) {
        return OTSColumn.fromNormalColumn(obj.getAsJsonPrimitive(NAME).getAsString());
    }
    
    @Override
    public JsonElement serialize(OTSColumn obj, Type t,
            JsonSerializationContext c) {
        JsonObject json = new JsonObject();

        switch (obj.getColumnType()) {
        case CONST:
            json.add(COLUMN_TYPE, new JsonPrimitive(OTSColumn.OTSColumnType.CONST.toString())); 
            serializeConstColumn(json, obj);
            break;
        case NORMAL:
            json.add(COLUMN_TYPE, new JsonPrimitive(OTSColumn.OTSColumnType.NORMAL.toString())); 
            serializeNormalColumn(json, obj);
            break;
        default:
            throw new IllegalArgumentException("Unsupport serialize the type : " + obj.getColumnType() + ""); 
        }
        return json;
    }

    @Override
    public OTSColumn deserialize(JsonElement ele, Type t,
            JsonDeserializationContext c) throws JsonParseException {
        JsonObject obj = ele.getAsJsonObject();
        String strColumnType = obj.getAsJsonPrimitive(COLUMN_TYPE).getAsString();
        OTSColumn.OTSColumnType columnType = OTSColumn.OTSColumnType.valueOf(strColumnType);

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
