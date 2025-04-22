package com.alibaba.datax.plugin.writer.oceanbasev10writer.common;

import java.util.concurrent.ConcurrentHashMap;

public class TableCache {
    private static final TableCache INSTANCE = new TableCache();
    private final ConcurrentHashMap<String, Table> TABLE_CACHE;

    private TableCache() {
        TABLE_CACHE = new ConcurrentHashMap<>();
    }

    public static TableCache getInstance() {
        return INSTANCE;
    }

    public Table getTable(String dbName, String tableName) {
        String fullTableName = String.join("-", dbName, tableName);
        return TABLE_CACHE.computeIfAbsent(fullTableName, (k) -> new Table(dbName, tableName));
    }
}