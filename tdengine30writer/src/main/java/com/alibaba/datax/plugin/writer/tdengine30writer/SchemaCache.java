package com.alibaba.datax.plugin.writer.tdengine30writer;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Schema cache for TDengine 3.X
 */
public final class SchemaCache {
    private static final Logger log = LoggerFactory.getLogger(TDengineWriter.Job.class);

    private static volatile SchemaCache instance;

    private static Configuration config;
    private static Connection conn;
    // table name -> TableMeta
    private static final Map<String, TableMeta> tableMetas = new LinkedHashMap<>();
    // table name ->List<ColumnMeta>
    private static final Map<String, List<ColumnMeta>> columnMetas = new LinkedHashMap<>();

    private SchemaCache(Configuration config) {
        SchemaCache.config = config;

        // connect
        final String user = config.getString(Key.USERNAME, Constants.DEFAULT_USERNAME);
        final String pass = config.getString(Key.PASSWORD, Constants.DEFAULT_PASSWORD);

        Configuration conn = Configuration.from(config.getList(Key.CONNECTION).get(0).toString());

        final String url = conn.getString(Key.JDBC_URL);
        try {
            SchemaCache.conn = DriverManager.getConnection(url, user, pass);
        } catch (SQLException e) {
            throw DataXException.asDataXException(
                    "failed to connect to url: " + url + ", cause: {" + e.getMessage() + "}");
        }

        final String dbname = TDengineWriter.parseDatabaseFromJdbcUrl(url);
        SchemaManager schemaManager = new Schema3_0Manager(SchemaCache.conn, dbname);

        // init table meta cache and load
        final List<String> tables = conn.getList(Key.TABLE, String.class);
        Map<String, TableMeta> tableMetas = schemaManager.loadTableMeta(tables);

        // init column meta cache
        SchemaCache.tableMetas.putAll(tableMetas);
        for (String table : tableMetas.keySet()) {
            SchemaCache.columnMetas.put(table, new ArrayList<>());
        }
    }

    public static SchemaCache getInstance(Configuration originConfig) {
        if (instance == null) {
            synchronized (SchemaCache.class) {
                if (instance == null) {
                    instance = new SchemaCache(originConfig);
                }
            }
        }
        return instance;
    }

    public TableMeta getTableMeta(String table_name) {
        return tableMetas.get(table_name);
    }

    public List<ColumnMeta> getColumnMetaList(String tbname) {
        if (columnMetas.get(tbname).isEmpty()) {
            synchronized (SchemaCache.class) {
                if (columnMetas.get(tbname).isEmpty()) {
                    List<String> column_name = config.getList(Key.COLUMN, String.class);

                    List<ColumnMeta> colMetaList = getColumnMetaListFromDb(tbname,
                            (colMeta) -> column_name.contains(colMeta.field));

                    columnMetas.get(tbname).addAll(colMetaList);
                }
            }
        }
        return columnMetas.get(tbname);
    }

    private List<ColumnMeta> getColumnMetaListFromDb(String tableName, Predicate<ColumnMeta> filter) {
        List<ColumnMeta> columnMetaList = new ArrayList<>();

        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("describe " + tableName);
            for (int i = 0; rs.next(); i++) {
                ColumnMeta columnMeta = buildColumnMeta(rs, i == 0);

                if (filter.test(columnMeta))
                    columnMetaList.add(columnMeta);
            }
            rs.close();
        } catch (SQLException e) {
            throw DataXException.asDataXException(TDengineWriterErrorCode.RUNTIME_EXCEPTION, e.getMessage());
        }

        for (ColumnMeta colMeta : columnMetaList) {
            if (!colMeta.isTag)
                continue;
            Object tagValue = getTagValue(tableName, colMeta.field);
            colMeta.value = tagValue;
        }

        return columnMetaList;
    }

    private Object getTagValue(String tableName, String tagName) {
        String sql = "select " + tagName + " from " + tableName;
        Object tagValue = null;
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                tagValue = rs.getObject(tagName);
            }
        } catch (SQLException e) {
            log.error("failed to get tag value, use NULL, cause: {" + e.getMessage() + "}");
        }

        return tagValue;
    }

    private ColumnMeta buildColumnMeta(ResultSet rs, boolean isPrimaryKey) throws SQLException {
        ColumnMeta columnMeta = new ColumnMeta();
        columnMeta.field = rs.getString(Constants.COLUMN_META_FIELD);
        columnMeta.type = rs.getString(Constants.COLUMN_META_TYPE);
        columnMeta.length = rs.getInt(Constants.COLUMN_META_LENGTH);
        columnMeta.note = rs.getString(Constants.COLUMN_META_NOTE);
        columnMeta.isTag = Constants.COLUMN_META_NOTE_TAG.equals(columnMeta.note);
        // columnMeta.isPrimaryKey = "ts".equals(columnMeta.field);
        columnMeta.isPrimaryKey = isPrimaryKey;
        return columnMeta;
    }

}
