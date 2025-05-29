package com.alibaba.datax.plugin.reader.rdbmswriter;

public class PostGisColumnTypeName {

    public static final String GEOMETRY = "geometry";

    public static final String TSRANGE = "tsrange";

    public static final String TSTZRANGE = "numrange";

    public static final String JSONB = "jsonb";

    public static final String INET = "inet";

    public static final String TSVECTOR = "tsvector";

    public static boolean isPGObject(String columnTypeName){
        return columnTypeName.equals(TSRANGE) ||
                columnTypeName.equals(TSTZRANGE) ||
                columnTypeName.equals(JSONB) ||
                columnTypeName.equals(INET) ||
                columnTypeName.equals(TSVECTOR);
    }
}
