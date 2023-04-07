package com.alibaba.datax.plugin.writer.tdenginewriter;

import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class SchemaManager {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaManager.class);
//    private static final String TAG_TABLE_NAME_MAP_KEY_SPLITTER = "_";
    private static final String TAG_TABLE_NAME_MAP_KEY_SPLITTER = "";

    private final Connection conn;
    private TimestampPrecision precision;
    private Map<String, Map<String, String>> tags2tbnameMaps = new HashMap<>();

    public SchemaManager(Connection conn) {
        this.conn = conn;
    }

    public TimestampPrecision loadDatabasePrecision() throws DataXException {
        if (this.precision != null)
            return this.precision;

        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select database()");
            String dbname = null;
            while (rs.next()) {
                dbname = rs.getString("database()");
            }
            if (dbname == null)
                throw DataXException.asDataXException(TDengineWriterErrorCode.RUNTIME_EXCEPTION,
                        "Database not specified or available");

            rs = stmt.executeQuery("show databases");
            while (rs.next()) {
                String name = rs.getString("name");
                if (!name.equalsIgnoreCase(dbname))
                    continue;
                String precision = rs.getString("precision");
                switch (precision) {
                    case "ns":
                        this.precision = TimestampPrecision.NANOSEC;
                        break;
                    case "us":
                        this.precision = TimestampPrecision.MICROSEC;
                        break;
                    case "ms":
                    default:
                        this.precision = TimestampPrecision.MILLISEC;
                }
            }
        } catch (SQLException e) {
            throw DataXException.asDataXException(TDengineWriterErrorCode.RUNTIME_EXCEPTION, e.getMessage());
        }
        return this.precision;
    }

    public Map<String, TableMeta> loadTableMeta(List<String> tables) throws DataXException {
        Map<String, TableMeta> tableMetas = new HashMap();

        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("show stables");
            while (rs.next()) {
                TableMeta tableMeta = buildSupTableMeta(rs);
                if (!tables.contains(tableMeta.tbname))
                    continue;
                tableMetas.put(tableMeta.tbname, tableMeta);
            }

            rs = stmt.executeQuery("show tables");
            while (rs.next()) {
                TableMeta tableMeta = buildSubTableMeta(rs);
                if (!tables.contains(tableMeta.tbname))
                    continue;
                tableMetas.put(tableMeta.tbname, tableMeta);
            }

            for (String tbname : tables) {
                if (!tableMetas.containsKey(tbname)) {
                    throw DataXException.asDataXException(TDengineWriterErrorCode.RUNTIME_EXCEPTION, "table metadata of " + tbname + " is empty!");
                }
            }
        } catch (SQLException e) {
            throw DataXException.asDataXException(TDengineWriterErrorCode.RUNTIME_EXCEPTION, e.getMessage());
        }
        return tableMetas;
    }

    public Map<String, List<ColumnMeta>> loadColumnMetas(List<String> tables) throws DataXException {
        Map<String, List<ColumnMeta>> ret = new HashMap<>();

        for (String table : tables) {
            List<ColumnMeta> columnMetaList = new ArrayList<>();
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("describe " + table);
                for (int i = 0; rs.next(); i++) {
                    ColumnMeta columnMeta = buildColumnMeta(rs, i == 0);
                    columnMetaList.add(columnMeta);
                }
            } catch (SQLException e) {
                throw DataXException.asDataXException(TDengineWriterErrorCode.RUNTIME_EXCEPTION, e.getMessage());
            }

            if (columnMetaList.isEmpty()) {
                LOG.error("column metadata of " + table + " is empty!");
                continue;
            }

            columnMetaList.stream().filter(colMeta -> colMeta.isTag).forEach(colMeta -> {
                String sql = "select " + colMeta.field + " from " + table;
                Object value = null;
                try (Statement stmt = conn.createStatement()) {
                    ResultSet rs = stmt.executeQuery(sql);
                    for (int i = 0; rs.next(); i++) {
                        value = rs.getObject(colMeta.field);
                        if (i > 0) {
                            value = null;
                            break;
                        }
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                colMeta.value = value;
            });

            LOG.debug("load column metadata of " + table + ": " + Arrays.toString(columnMetaList.toArray()));
            ret.put(table, columnMetaList);
        }
        return ret;
    }

    private TableMeta buildSupTableMeta(ResultSet rs) throws SQLException {
        TableMeta tableMeta = new TableMeta();
        tableMeta.tableType = TableType.SUP_TABLE;
        tableMeta.tbname = rs.getString("name");
        tableMeta.columns = rs.getInt("columns");
        tableMeta.tags = rs.getInt("tags");
        tableMeta.tables = rs.getInt("tables");

        LOG.debug("load table metadata of " + tableMeta.tbname + ": " + tableMeta);
        return tableMeta;
    }

    private TableMeta buildSubTableMeta(ResultSet rs) throws SQLException {
        TableMeta tableMeta = new TableMeta();
        String stable_name = rs.getString("stable_name");
        tableMeta.tableType = StringUtils.isBlank(stable_name) ? TableType.NML_TABLE : TableType.SUB_TABLE;
        tableMeta.tbname = rs.getString("table_name");
        tableMeta.columns = rs.getInt("columns");
        tableMeta.stable_name = StringUtils.isBlank(stable_name) ? null : stable_name;

        LOG.debug("load table metadata of " + tableMeta.tbname + ": " + tableMeta);
        return tableMeta;
    }

    private ColumnMeta buildColumnMeta(ResultSet rs, boolean isPrimaryKey) throws SQLException {
        ColumnMeta columnMeta = new ColumnMeta();
        columnMeta.field = rs.getString("Field");
        columnMeta.type = rs.getString("Type");
        columnMeta.length = rs.getInt("Length");
        columnMeta.note = rs.getString("Note");
        columnMeta.isTag = columnMeta.note != null && columnMeta.note.equals("TAG");
        columnMeta.isPrimaryKey = isPrimaryKey;
        return columnMeta;
    }

    public Map<String, String> loadTagTableNameMap(String table) throws SQLException {
        if (tags2tbnameMaps.containsKey(table))
            return tags2tbnameMaps.get(table);
        Map<String, String> tags2tbname = new HashMap<>();
        try (Statement stmt = conn.createStatement()) {
            // describe table
            List<String> tags = new ArrayList<>();
            ResultSet rs = stmt.executeQuery("describe " + table);
            while (rs.next()) {
                String note = rs.getString("Note");
                if ("TAG".equals(note)) {
                    tags.add(rs.getString("Field"));
                }
            }
            // select distinct tbname, t1, t2 from stb
            rs = stmt.executeQuery("select distinct " + String.join(",", tags) + ",tbname from " + table);
            while (rs.next()) {
                ResultSet finalRs = rs;
                String tagStr = tags.stream().map(t -> {
                    try {
                        return finalRs.getString(t);
                    } catch (SQLException e) {
                        LOG.error(e.getMessage(), e);
                    }
                    return "NULL";
                }).collect(Collectors.joining(TAG_TABLE_NAME_MAP_KEY_SPLITTER));
                String tbname = rs.getString("tbname");
                tags2tbname.put(tagStr, tbname);
            }
        }
        tags2tbnameMaps.put(table, tags2tbname);
        return tags2tbname;
    }
}
