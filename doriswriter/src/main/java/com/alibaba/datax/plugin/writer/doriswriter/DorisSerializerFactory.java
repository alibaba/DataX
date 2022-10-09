package com.alibaba.datax.plugin.writer.doriswriter;

import java.util.Map;

public class DorisSerializerFactory {
    public DorisSerializerFactory(){

    }
    public static DorisSerializer createSerializer(DorisWriterOptions writerOptions) {
        if (DorisWriterOptions.StreamLoadFormat.CSV.equals(writerOptions.getStreamLoadFormat())) {
            Map<String, Object> props = writerOptions.getLoadProps();
            return new DorisCsvSerializer(null == props || !props.containsKey("column_separator") ? null : String.valueOf(props.get("column_separator")));
        }
        if (DorisWriterOptions.StreamLoadFormat.JSON.equals(writerOptions.getStreamLoadFormat())) {
            return new DorisJsonSerializer(writerOptions.getColumns());
        }
        throw new RuntimeException("Failed to create row serializer, unsupported `format` from stream load properties.");
    }
}
