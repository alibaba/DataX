package com.alibaba.datax.plugin.reader.otsreader.adaptor;

import java.lang.reflect.Type;

import com.aliyun.openservices.ots.model.ColumnType;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * {"type":"INF_MIN", "value":""}
 * {"type":"INF_MAX", "value":""}
 * {"type":"STRING", "value":"hello"}
 * {"type":"INTEGER", "value":"1222"}
 */
public class PrimaryKeyValueAdaptor implements JsonDeserializer<PrimaryKeyValue>, JsonSerializer<PrimaryKeyValue>{
    private final static String TYPE = "type";
    private final static String VALUE = "value";
    private final static String INF_MIN = "INF_MIN";
    private final static String INF_MAX = "INF_MAX";

    @Override
    public JsonElement serialize(PrimaryKeyValue obj, Type t,
            JsonSerializationContext c) {
        JsonObject json = new JsonObject();
        
        if (obj == PrimaryKeyValue.INF_MIN) {
            json.add(TYPE, new JsonPrimitive(INF_MIN)); 
            json.add(VALUE, new JsonPrimitive(""));
            return json;
        }
        
        if (obj == PrimaryKeyValue.INF_MAX) {
            json.add(TYPE, new JsonPrimitive(INF_MAX)); 
            json.add(VALUE, new JsonPrimitive(""));
            return json;
        }

        switch (obj.getType()) {
        case STRING : 
            json.add(TYPE, new JsonPrimitive(ColumnType.STRING.toString())); 
            json.add(VALUE, new JsonPrimitive(obj.asString()));
            break;
        case INTEGER : 
            json.add(TYPE, new JsonPrimitive(ColumnType.INTEGER.toString())); 
            json.add(VALUE, new JsonPrimitive(obj.asLong()));
            break;
        default:
            throw new IllegalArgumentException("Unsupport serialize the type : " + obj.getType() + "");
        }
        return json;
    }

    @Override
    public PrimaryKeyValue deserialize(JsonElement ele, Type t,
            JsonDeserializationContext c) throws JsonParseException {

        JsonObject obj = ele.getAsJsonObject();
        String strType = obj.getAsJsonPrimitive(TYPE).getAsString();
        JsonPrimitive jsonValue =  obj.getAsJsonPrimitive(VALUE);
        
        if (strType.equals(INF_MIN)) {
            return PrimaryKeyValue.INF_MIN;
        }
        
        if (strType.equals(INF_MAX)) {
            return PrimaryKeyValue.INF_MAX;
        }
        
        PrimaryKeyValue value = null;
        PrimaryKeyType type = PrimaryKeyType.valueOf(strType);
        switch(type) {
        case STRING : 
            value = PrimaryKeyValue.fromString(jsonValue.getAsString());
            break;
        case INTEGER : 
            value = PrimaryKeyValue.fromLong(jsonValue.getAsLong());
            break;
        default:
            throw new IllegalArgumentException("Unsupport deserialize the type : " + type + "");
        }
        return value;
    }
}
