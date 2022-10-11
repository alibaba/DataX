package com.starrocks.connector.datax.plugin.writer.starrockswriter.row;

import java.util.Map;

import com.starrocks.connector.datax.plugin.writer.starrockswriter.StarRocksWriterOptions;

public class StarRocksSerializerFactory {

    private StarRocksSerializerFactory() {}

    public static StarRocksISerializer createSerializer(StarRocksWriterOptions writerOptions) {
        if (StarRocksWriterOptions.StreamLoadFormat.CSV.equals(writerOptions.getStreamLoadFormat())) {
            Map<String, Object> props = writerOptions.getLoadProps();
            return new StarRocksCsvSerializer(null == props || !props.containsKey("column_separator") ? null : String.valueOf(props.get("column_separator")));
        }
        if (StarRocksWriterOptions.StreamLoadFormat.JSON.equals(writerOptions.getStreamLoadFormat())) {
            return new StarRocksJsonSerializer(writerOptions.getColumns());
        }
        throw new RuntimeException("Failed to create row serializer, unsupported `format` from stream load properties.");
    }
    
}
