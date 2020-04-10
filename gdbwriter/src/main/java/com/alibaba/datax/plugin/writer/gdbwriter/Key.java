package com.alibaba.datax.plugin.writer.gdbwriter;

public final class Key {

    /**
     * 此处声明插件用到的需要插件使用者提供的配置项
     */

    public final static String HOST = "host";
    public final static String PORT = "port";
    public final static String USERNAME = "username";
    public static final String PASSWORD = "password";

    /**
     * import type and mode
     */
    public static final String IMPORT_TYPE = "labelType";
    public static final String UPDATE_MODE = "writeMode";

    /**
     * label prefix issue
     */
    public static final String ID_TRANS_RULE = "idTransRule";
    public static final String SRC_ID_TRANS_RULE = "srcIdTransRule";
    public static final String DST_ID_TRANS_RULE = "dstIdTransRule";

    public static final String LABEL = "label";
    public static final String SRC_LABEL = "srcLabel";
    public static final String DST_LABEL = "dstLabel";

    public static final String MAPPING = "mapping";

    /**
     * column define in Gdb
     */
    public static final String COLUMN = "column";
    public static final String COLUMN_NAME = "name";
    public static final String COLUMN_VALUE = "value";
    public static final String COLUMN_TYPE = "type";
    public static final String COLUMN_NODE_TYPE = "columnType";

    /**
     * Gdb Vertex/Edge elements
     */
    public static final String ID = "id";
    public static final String FROM = "from";
    public static final String TO = "to";
    public static final String PROPERTIES = "properties";
    public static final String PROP_KEY = "name";
    public static final String PROP_VALUE = "value";
    public static final String PROP_TYPE = "type";

    public static final String PROPERTIES_JSON_STR = "propertiesJsonStr";
    public static final String MAX_PROPERTIES_BATCH_NUM = "maxPropertiesBatchNumber";

    /**
     * session less client configure for connect pool
     */
    public static final String MAX_IN_PROCESS_PER_CONNECTION = "maxInProcessPerConnection";
    public static final String MAX_CONNECTION_POOL_SIZE = "maxConnectionPoolSize";
    public static final String MAX_SIMULTANEOUS_USAGE_PER_CONNECTION = "maxSimultaneousUsagePerConnection";

    public static final String MAX_RECORDS_IN_BATCH = "maxRecordsInBatch";
    public static final String SESSION_STATE = "session";

    /**
     * request length limit, include gdb element string length GDB字段长度限制配置，可分别配置各字段的限制，超过限制的记录会当脏数据处理
     */
    public static final String MAX_GDB_STRING_LENGTH = "maxStringLengthLimit";
    public static final String MAX_GDB_ID_LENGTH = "maxIdStringLengthLimit";
    public static final String MAX_GDB_LABEL_LENGTH = "maxLabelStringLengthLimit";
    public static final String MAX_GDB_PROP_KEY_LENGTH = "maxPropKeyStringLengthLimit";
    public static final String MAX_GDB_PROP_VALUE_LENGTH = "maxPropValueStringLengthLimit";

    public static final String MAX_GDB_REQUEST_LENGTH = "maxRequestLengthLimit";

    public static enum ImportType {
        /**
         * Import vertices
         */
        VERTEX,
        /**
         * Import edges
         */
        EDGE;
    }

    public static enum UpdateMode {
        /**
         * Insert new records, fail if exists
         */
        INSERT,
        /**
         * Skip this record if exists
         */
        SKIP,
        /**
         * Update property of this record if exists
         */
        MERGE;
    }

    public static enum ColumnType {
        /**
         * vertex or edge id
         */
        primaryKey,

        /**
         * vertex property
         */
        vertexProperty,

        /**
         * vertex setProperty
         */
        vertexSetProperty,

        /**
         * start vertex id of edge
         */
        srcPrimaryKey,

        /**
         * end vertex id of edge
         */
        dstPrimaryKey,

        /**
         * edge property
         */
        edgeProperty,

        /**
         * vertex json style property
         */
        vertexJsonProperty,

        /**
         * edge json style property
         */
        edgeJsonProperty
    }

    public static enum IdTransRule {
        /**
         * vertex or edge id with 'label' prefix
         */
        labelPrefix,

        /**
         * vertex or edge id raw
         */
        none
    }

    public static enum PropertyType {
        /**
         * single Vertex Property
         */
        single,

        /**
         * set Vertex Property
         */
        set
    }

}
