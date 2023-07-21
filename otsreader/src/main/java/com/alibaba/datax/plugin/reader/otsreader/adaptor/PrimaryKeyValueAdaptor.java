package com.alibaba.datax.plugin.reader.otsreader.adaptor;

import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.google.gson.*;
import org.apache.commons.codec.binary.Base64;

import java.lang.reflect.Type;

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
        
        if (obj.isInfMin()) {
            json.add(TYPE, new JsonPrimitive(INF_MIN)); 
            return json;
        }
        
        if (obj.isInfMax()) {
            json.add(TYPE, new JsonPrimitive(INF_MAX)); 
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
        case BINARY : 
            json.add(TYPE, new JsonPrimitive(ColumnType.BINARY.toString())); 
            json.add(VALUE, new JsonPrimitive(Base64.encodeBase64String(obj.asBinary())));
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
        
        if (strType.equalsIgnoreCase(INF_MIN)) {
            return PrimaryKeyValue.INF_MIN;
        }
        
        if (strType.equalsIgnoreCase(INF_MAX)) {
            return PrimaryKeyValue.INF_MAX;
        }
        
        JsonPrimitive jsonValue =  obj.getAsJsonPrimitive(VALUE);
        
        PrimaryKeyValue value = null;
        PrimaryKeyType type = PrimaryKeyType.valueOf(strType);
        switch(type) {
        case STRING : 
            value = PrimaryKeyValue.fromString(jsonValue.getAsString());
            break;
        case INTEGER : 
            value = PrimaryKeyValue.fromLong(jsonValue.getAsLong());
            break;
        case BINARY : 
            value = PrimaryKeyValue.fromBinary(Base64.decodeBase64(jsonValue.getAsString()));
            break;
        default:
            throw new IllegalArgumentException("Unsupport deserialize the type : " + type + "");
        }
        return value;
    }
}
