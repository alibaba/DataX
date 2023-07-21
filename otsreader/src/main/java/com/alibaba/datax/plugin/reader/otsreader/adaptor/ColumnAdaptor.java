package com.alibaba.datax.plugin.reader.otsreader.adaptor;

import com.alibaba.datax.common.element.*;
import com.google.gson.*;
import org.apache.commons.codec.binary.Base64;

import java.lang.reflect.Type;

public class ColumnAdaptor implements JsonDeserializer<Column>, JsonSerializer<Column>{
    private final static String TYPE = "type";
    private final static String RAW = "rawData";

    @Override
    public JsonElement serialize(Column obj, Type t,
            JsonSerializationContext c) {
        JsonObject json = new JsonObject();
        
        String rawData = null;
        switch (obj.getType()){
            case BOOL:
                rawData = String.valueOf(obj.getRawData()); break;
            case BYTES:
                rawData = Base64.encodeBase64String((byte[]) obj.getRawData()); break;
            case DOUBLE:
                rawData = String.valueOf(obj.getRawData());break;
            case LONG:
                rawData = String.valueOf(obj.getRawData());break;
            case STRING:
                rawData = String.valueOf(obj.getRawData());break;
            default:
                throw new IllegalArgumentException("Unsupport parse the column type:" + obj.getType().toString());
            
        }
        json.add(TYPE, new JsonPrimitive(obj.getType().toString())); 
        json.add(RAW, new JsonPrimitive(rawData)); 
        return json;
    }

    @Override
    public Column deserialize(JsonElement ele, Type t,
            JsonDeserializationContext c) throws JsonParseException {
        JsonObject obj = ele.getAsJsonObject();
        
        String strType = obj.getAsJsonPrimitive(TYPE).getAsString();
        String strRaw = obj.getAsJsonPrimitive(RAW).getAsString();
        Column.Type type = Column.Type.valueOf(strType);
        switch (type){
            case BOOL:
                return new BoolColumn(strRaw);
            case BYTES:
                return new BytesColumn(Base64.decodeBase64(strRaw));
            case DOUBLE:
                return new DoubleColumn(strRaw);
            case LONG:
                return new LongColumn(strRaw);
            case STRING:
                return new StringColumn(strRaw);
            default:
                throw new IllegalArgumentException("Unsupport parse the column type:" + type.toString());
            
        }
    }
}
