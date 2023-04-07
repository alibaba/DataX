package com.alibaba.datax.plugin.writer.selectdbwriter;

import java.util.Map;

public class SelectdbCodecFactory {
    public SelectdbCodecFactory (){

    }
    public static SelectdbCodec createCodec( Keys writerOptions) {
        if ( Keys.StreamLoadFormat.CSV.equals(writerOptions.getStreamLoadFormat())) {
            Map<String, Object> props = writerOptions.getLoadProps();
            return new SelectdbCsvCodec (null == props || !props.containsKey("file.column_separator") ? null : String.valueOf(props.get("file.column_separator")));
        }
        if ( Keys.StreamLoadFormat.JSON.equals(writerOptions.getStreamLoadFormat())) {
            return new SelectdbJsonCodec (writerOptions.getColumns());
        }
        throw new RuntimeException("Failed to create row serializer, unsupported `format` from stream load properties.");
    }
}
