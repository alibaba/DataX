package com.alibaba.datax.plugin.writer.tdengine30writer;

import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 适配 TDengine 3.0 的 SchemaManager
 */
public class Schema3_0Manager extends SchemaManager {

    private static final Logger LOG = LoggerFactory.getLogger(Schema3_0Manager.class);

    private final String dbname;

    public Schema3_0Manager(Connection conn, String dbname) {
        super(conn);
        this.dbname = dbname;
    }

    @Override
    public Map<String, TableMeta> loadTableMeta(List<String> tables) throws DataXException {
        Map<String, TableMeta> tableMetas = new HashMap<>();

        try (Statement stmt = conn.createStatement()) {

            StringBuilder sb = new StringBuilder();
            sb.append("select * from ")
              .append(Constants.INFORMATION_SCHEMA)
              .append(Constants.INFORMATION_SCHEMA_COMMA)
              .append(Constants.INFORMATION_SCHEMA_TABLE_INS_STABLES)
              .append(" where db_name = ")
              .append(getDbnameForSqlQuery())
              .append(" and stable_name in ")
              .append(tables.stream().map(t -> "'" + t + "'").collect(Collectors.joining(",", "(", ")")));

            ResultSet rs = stmt.executeQuery(sb.toString());
            while (rs.next()) {
                TableMeta tableMeta = buildSupTableMeta(rs);
                if (!tables.contains(tableMeta.tbname))
                    continue;
                tableMetas.put(tableMeta.tbname, tableMeta);
            }

            sb = new StringBuilder();
            sb.append("select * from ")
              .append(Constants.INFORMATION_SCHEMA)
              .append(Constants.INFORMATION_SCHEMA_COMMA)
              .append(Constants.INFORMATION_SCHEMA_TABLE_INS_TABLES)
              .append(" where db_name = ")
              .append(getDbnameForSqlQuery())
              .append(" and table_name in ")
              .append(tables.stream().map(t -> "'" + t + "'").collect(Collectors.joining(",", "(", ")")));

            rs = stmt.executeQuery(sb.toString());
            while (rs.next()) {
                TableMeta tableMeta = buildSubTableMeta(rs);
                if (!tables.contains(tableMeta.tbname))
                    continue;
                tableMetas.put(tableMeta.tbname, tableMeta);
            }

            for (String tbname : tables) {
                if (!tableMetas.containsKey(tbname)) {
                    throw DataXException.asDataXException(TDengineWriterErrorCode.RUNTIME_EXCEPTION,
                            "table metadata of " + tbname + " is empty!");
                }
            }
        } catch (SQLException e) {
            throw DataXException.asDataXException(TDengineWriterErrorCode.RUNTIME_EXCEPTION, e.getMessage());
        }
        return tableMetas;
    }

    private String getDbnameForSqlQuery() {
        return "\"" + dbname + "\"";
    }

    @Override
    protected TableMeta buildSupTableMeta(ResultSet rs) throws SQLException {
        TableMeta tableMeta = new TableMeta();
        tableMeta.tableType = TableType.SUP_TABLE;
        tableMeta.tbname = rs.getString(Constants.TABLE_META_SUP_TABLE_NAME);
        tableMeta.columns = rs.getInt(Constants.TABLE_META_COLUMNS);
        tableMeta.tags = rs.getInt(Constants.TABLE_META_TAGS);
        //        tableMeta.tables = rs.getInt("tables"); // 直接从 ins_stables 查不到子表数量
        LOG.debug("load table metadata of " + tableMeta.tbname + ": " + tableMeta);
        return tableMeta;
    }

    @Override
    protected TableMeta buildSubTableMeta(ResultSet rs) throws SQLException {
        TableMeta tableMeta = new TableMeta();
        String stable_name = rs.getString(Constants.TABLE_META_SUP_TABLE_NAME);
        tableMeta.tableType = StringUtils.isBlank(stable_name) ? TableType.NML_TABLE : TableType.SUB_TABLE;
        tableMeta.tbname = rs.getString(Constants.TABLE_META_TABLE_NAME);
        tableMeta.columns = rs.getInt(Constants.TABLE_META_COLUMNS);
        tableMeta.stable_name = StringUtils.isBlank(stable_name) ? null : stable_name;
        LOG.debug("load table metadata of " + tableMeta.tbname + ": " + tableMeta);
        return tableMeta;
    }

    @Override
    protected ColumnMeta buildColumnMeta(ResultSet rs, boolean isPrimaryKey) throws SQLException {
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

    @Override
    public TimestampPrecision loadDatabasePrecision() throws DataXException {
        if (this.precision != null)
            return this.precision;
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "select * from " + Constants.INFORMATION_SCHEMA + Constants.INFORMATION_SCHEMA_COMMA +
                            Constants.INFORMATION_SCHEMA_TABLE_INS_DATABASES + " where name = " +
                            getDbnameForSqlQuery());
            while (rs.next()) {
                String precision = rs.getString(Constants.DATABASE_META_PRECISION);
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

    @Override
    public Map<String, String> loadTagTableNameMap(String table) throws SQLException {
        if (tags2tbnameMaps.containsKey(table))
            return tags2tbnameMaps.get(table);
        Map<String, String> tags2tbname = new HashMap<>();
        try (Statement stmt = conn.createStatement()) {
            // describe table
            List<String> tags = new ArrayList<>();
            ResultSet rs = stmt.executeQuery("describe " + table);
            while (rs.next()) {
                String note = rs.getString(Constants.COLUMN_META_NOTE);
                if (Constants.COLUMN_META_NOTE_TAG.equals(note)) {
                    tags.add(rs.getString(Constants.COLUMN_META_FIELD));
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
