package com.alibaba.datax.plugin.writer.doriswriter;

import java.util.Map;

public class DorisCodecFactory {
    public DorisCodecFactory (){

    }
    public static DorisCodec createCodec( Keys writerOptions) {
        if ( Keys.StreamLoadFormat.CSV.equals(writerOptions.getStreamLoadFormat())) {
            Map<String, Object> props = writerOptions.getLoadProps();
            return new DorisCsvCodec (null == props || !props.containsKey("column_separator") ? null : String.valueOf(props.get("column_separator")));
        }

        throw new RuntimeException("Failed to create row serializer, unsupported `format` from stream load properties.");
    }
}
