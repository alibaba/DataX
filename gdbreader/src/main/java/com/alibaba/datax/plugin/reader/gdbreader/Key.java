package com.alibaba.datax.plugin.reader.gdbreader;

public final class Key {

    /**
     * 此处声明插件用到的需要插件使用者提供的配置项
     */
    public final static String HOST = "host";
    public final static String PORT = "port";
    public final static String USERNAME = "username";
    public static final String PASSWORD = "password";

    public static final String LABEL = "labels";
    public static final String EXPORT_TYPE = "labelType";

    public static final String RANGE_SPLIT_SIZE = "RangeSplitSize";
    public static final String FETCH_BATCH_SIZE = "fetchBatchSize";

    public static final String COLUMN = "column";
    public static final String COLUMN_NAME = "name";
    public static final String COLUMN_TYPE = "type";
    public static final String COLUMN_NODE_TYPE = "columnType";

    public enum ExportType {
        /**
         * Import vertices
         */
        VERTEX,
        /**
         * Import edges
         */
        EDGE
    }

    public enum ColumnType {
        /**
         * vertex or edge id
         */
        primaryKey,

        /**
         * vertex or edge label
         */
        primaryLabel,

        /**
         * vertex property
         */
        vertexProperty,

        /**
         * collects all vertex property to Json list
         */
        vertexJsonProperty,

        /**
         * start vertex id of edge
         */
        srcPrimaryKey,

        /**
         * start vertex label of edge
         */
        srcPrimaryLabel,

        /**
         * end vertex id of edge
         */
        dstPrimaryKey,

        /**
         * end vertex label of edge
         */
        dstPrimaryLabel,

        /**
         * edge property
         */
        edgeProperty,

        /**
         * collects all edge property to Json list
         */
        edgeJsonProperty,
    }
}
