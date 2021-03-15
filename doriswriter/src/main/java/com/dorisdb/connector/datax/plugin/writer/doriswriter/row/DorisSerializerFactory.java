package com.dorisdb.connector.datax.plugin.writer.doriswriter.row;

import java.util.Map;

import com.dorisdb.connector.datax.plugin.writer.doriswriter.DorisWriterOptions;

public class DorisSerializerFactory {

    private DorisSerializerFactory() {}

    public static DorisISerializer createSerializer(DorisWriterOptions writerOptions) {
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
