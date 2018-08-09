package com.alibaba.datax.plugin.writer.kuduwriter;

public class Key {
    /**
     * mongodb 的 host 地址
     */
    public static final String KUDU_MASTERS = "kudu_masters";
    /**
     * Kudu表名
     */
    public static final String TABLE_NAME = "tableName";
    /**
     * mongodb 的列
     */
    public static final String KUDU_COLUMN = "column";
    /**
     * 每个列的名字
     */
    public static final String COLUMN_NAME = "name";
    /**
     * 每个列的类型
     */
    public static final String COLUMN_TYPE = "type";
    /**
     * 批量抽取的数据大小
     */
    public static final String BATCH_SIZE = "batch_size";
    /**
     * 是否启用upsert方法
     */
    public static final String IS_UPSERT = "isUpsert";
}
