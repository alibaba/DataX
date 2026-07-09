package com.alibaba.datax.plugin.writer.tdengine30writer;

public class Constants {
    public static final String DEFAULT_USERNAME = "root";
    public static final String DEFAULT_PASSWORD = "taosdata";
    public static final int DEFAULT_BATCH_SIZE = 1;
    public static final boolean DEFAULT_IGNORE_TAGS_UNMATCHED = false;

    // ----------------- tdengine version -------------------
    public static final String SERVER_VERSION_2 = "2";
    public static final String SERVER_VERSION_3 = "3";
    public static final String SERVER_VERSION = "server_version()";

    // ----------------- schema -------------------
    public static final String INFORMATION_SCHEMA = "information_schema";
    public static final String INFORMATION_SCHEMA_TABLE_INS_DATABASES = "ins_databases";
    public static final String INFORMATION_SCHEMA_TABLE_INS_STABLES = "ins_stables";
    public static final String INFORMATION_SCHEMA_TABLE_INS_TABLES = "ins_tables";
    public static final String INFORMATION_SCHEMA_COMMA = ".";

    // ----------------- table meta -------------------
    public static final String TABLE_META_DB_NAME = "db_name";
    public static final String TABLE_META_SUP_TABLE_NAME = "stable_name";
    public static final String TABLE_META_TABLE_NAME = "table_name";
    public static final String TABLE_META_COLUMNS = "columns";
    public static final String TABLE_META_TAGS = "tags";

    public static final String COLUMN_META_FIELD = "field";
    public static final String COLUMN_META_TYPE = "type";
    public static final String COLUMN_META_LENGTH = "length";
    public static final String COLUMN_META_NOTE = "note";

    public static final String COLUMN_META_NOTE_TAG = "TAG";

    // ----------------- database meta -------------------
    public static final String DATABASE_META_PRECISION = "precision";

}