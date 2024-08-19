package com.alibaba.datax.plugin.reader.obhbasereader.task;

import static com.alibaba.datax.plugin.reader.obhbasereader.Constant.OB_READ_HINT;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.reader.obhbasereader.Constant;
import com.alibaba.datax.plugin.reader.obhbasereader.HbaseColumnCell;
import com.alibaba.datax.plugin.reader.obhbasereader.HbaseReaderErrorCode;
import com.alibaba.datax.plugin.reader.obhbasereader.Key;
import com.alibaba.datax.plugin.reader.obhbasereader.enums.FetchVersion;
import com.alibaba.datax.plugin.reader.obhbasereader.util.ObHbaseReaderUtil;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.ObReaderUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SQLNormalModeReader extends AbstractHbaseTask {
    private final static String QUERY_SQL_TEMPLATE = "select %s K, Q, T, V, hex(K) as `hex` from %s %s";
    private static Logger LOG = LoggerFactory.getLogger(SQLNormalModeReader.class);
    private final Map<String, byte[]> columnMap;
    private final Map<String, Long> versionMap;
    private final FetchVersion fetchVersion;
    private Set<String> columnNames;
    private boolean noMoreData = false;
    private String querySQL = null;
    private Connection conn = null;
    private PreparedStatement stmt = null;
    private ResultSet rs = null;
    private String jdbcUrl = null;
    private String columnFamily = null;
    private String username = null;
    private String password = null;
    private int fetchSize = com.alibaba.datax.plugin.reader.obhbasereader.Constant.DEFAULT_FETCH_SIZE;
    private long readBatchSize = com.alibaba.datax.plugin.reader.obhbasereader.Constant.DEFAULT_READ_BATCH_SIZE;
    private Configuration configuration;
    private boolean hasRange = false;
    private String[] savepoint = new String[3];
    // only used by unit test
    protected boolean reuseConn = false;

    public SQLNormalModeReader(Configuration configuration) {
        this.configuration = configuration;
        this.hbaseColumnCellMap = ObHbaseReaderUtil.parseColumn(configuration.getList(Key.COLUMN, Map.class));
        if (hbaseColumnCellMap.size() == 0) {
            LOG.error("no column cells specified.");
            throw new RuntimeException("no column cells specified");
        }
        columnFamily = ObHbaseReaderUtil.parseColumnFamily(hbaseColumnCellMap.values());
        this.columnNames =
                hbaseColumnCellMap.keySet().stream().map(e -> ObHbaseReaderUtil.isRowkeyColumn(e) ? Constant.ROWKEY_FLAG : e.substring((columnFamily + ":").length())).collect(Collectors.toSet());

        String partInfo = "";
        String partName = configuration.getString(Key.PARTITION_NAME, null);
        if (partName != null) {
            partInfo = "partition(" + partName + ")";
        }

        String tableName = configuration.getString(Key.TABLE, null);
        String hint = configuration.getString(Key.READER_HINT, OB_READ_HINT);
        this.hasRange = !StringUtils.isEmpty(configuration.getString(Key.RANGE, null));
        this.querySQL = String.format(QUERY_SQL_TEMPLATE, hint, tableName + "$" + columnFamily, partInfo);
        if (hasRange) {
            this.querySQL = querySQL + " where (" + configuration.getString(Key.RANGE) + ")";
        }
        this.jdbcUrl = configuration.getString(Key.JDBC_URL, null);
        this.username = configuration.getString(Key.USERNAME, null);
        this.password = configuration.getString(Key.PASSWORD, null);
        this.columnMap = Maps.newHashMap();
        this.versionMap = Maps.newHashMap();
        this.fetchVersion = FetchVersion.getByDesc(configuration.getString("version", FetchVersion.LATEST.name()));
        this.timezone = configuration.getString(Key.TIMEZONE, "UTC");
        this.encoding = configuration.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);
        this.fetchSize = configuration.getInt(Key.FETCH_SIZE, com.alibaba.datax.plugin.reader.obhbasereader.Constant.DEFAULT_FETCH_SIZE);
        this.readBatchSize = configuration.getLong(Key.READ_BATCH_SIZE, com.alibaba.datax.plugin.reader.obhbasereader.Constant.DEFAULT_READ_BATCH_SIZE);
        LOG.info("read from jdbcUrl {} with fetchSize {}, readBatchSize {}", jdbcUrl, fetchSize, readBatchSize);
    }

    private boolean notFinished(String currentKey) throws SQLException {
        boolean updateSuccess = updateResultSet();
        if (updateSuccess) {
            String newKey = rs.getString("K");
            return newKey.equals(currentKey);
        } else {
            noMoreData = true;
            Arrays.fill(savepoint, null);
            return false;
        }
    }

    private boolean updateResultSet() throws SQLException {
        if (rs != null && rs.next()) {
            return true;
        }
        if (savepoint[0] != null) {
            int retryLimit = 10;
            int retryCount = 0;
            String tempQuery = querySQL + (hasRange ? " and " : " where ") + "(K,Q,T) > (unhex(?),?,?) order by K,Q,T limit " + readBatchSize;
            while (retryCount < retryLimit) {
                retryCount++;
                try {
                    resetConnection();
                    DBUtil.closeDBResources(rs, stmt, null);
                    stmt = conn.prepareStatement(tempQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    stmt.setFetchSize(fetchSize);
                    for (int i = 0; i < savepoint.length; i++) {
                        stmt.setObject(i + 1, savepoint[i]);
                    }
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        LOG.info("execute sql: {}, savepoint:[{}]", tempQuery, Arrays.stream(savepoint).map(e -> "'" + e + "'").collect(Collectors.joining(",")));
                        return true;
                    }
                    // All data in this task are read
                    break;
                } catch (Exception ex) {
                    LOG.error("failed to query sql, will retry {} times", retryCount, ex);
                    DBUtil.closeDBResources(rs, stmt, conn);
                    if (retryCount > retryLimit) {
                        LOG.error("Sql: [{}] executed failed, savepoint:[{}], reason: {}", tempQuery, Arrays.stream(savepoint).map(e -> "'" + e + "'").collect(Collectors.joining(",")),
                                ex.getMessage());
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
        return false;
    }

    @Override
    public void prepare() {
        int retryLimit = 10;
        int retryCount = 0;
        while (true) {
            retryCount++;
            try {
                resetConnection();
                String tempQuery = querySQL + " order by K,Q,T limit " + readBatchSize;
                stmt = conn.prepareStatement(tempQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                stmt.setFetchSize(fetchSize);
                LOG.info("execute sql : {}", tempQuery);
                rs = stmt.executeQuery();
                if (!rs.next()) {
                    noMoreData = true;
                }
                break;
            } catch (Exception e) {
                LOG.error("failed to query sql, will retry {} times", retryCount, e);
                DBUtil.closeDBResources(rs, stmt, conn);
                if (retryCount > retryLimit) {
                    LOG.error("Sql: [{}] executed failed, reason: {}", querySQL, e.getMessage());
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public boolean fetchLine(Record record) throws Exception {
        try {
            if (noMoreData) {
                return false;
            }
            String currentKey = rs.getString("K");
            savepoint[0] = rs.getString("hex");
            columnMap.put(Constant.ROWKEY_FLAG, currentKey.getBytes());
            do {
                String columnName = rs.getString("Q");
                savepoint[1] = columnName;
                if (!this.columnNames.contains(columnName)) {
                    continue;
                }
                Long version = rs.getLong("T");
                savepoint[2] = String.valueOf(version);
                byte[] value = rs.getBytes("V");
                Predicate<Long> predicate;
                switch (this.fetchVersion) {
                    case OLDEST:
                        predicate = v -> v.compareTo(versionMap.getOrDefault(columnName, Long.MIN_VALUE)) > 0;
                        break;
                    case LATEST:
                        predicate = v -> v.compareTo(versionMap.getOrDefault(columnName, Long.MAX_VALUE)) < 0;
                        break;
                    default:
                        throw DataXException.asDataXException(HbaseReaderErrorCode.ILLEGAL_VALUE, "Not support version: " + this.fetchVersion);
                }

                if (predicate.test(version)) {
                    versionMap.put(columnName, version);
                    columnMap.put(columnName, value);
                }
            } while (notFinished(currentKey));

            for (HbaseColumnCell cell : this.hbaseColumnCellMap.values()) {
                Column column = null;
                if (cell.isConstant()) {
                    // 对常量字段的处理
                    column = this.constantMap.get(cell.getColumnName());
                } else {
                    String columnName = ObHbaseReaderUtil.isRowkeyColumn(cell.getColumnName()) ? Constant.ROWKEY_FLAG : cell.getColumnName().substring((columnFamily + ":").length());
                    byte[] value = null;
                    if (!columnMap.containsKey(columnName)) {
                        LOG.debug("{} is not contained in the record with K value={}. consider this record as null record.", columnName, currentKey);
                    } else {
                        value = columnMap.get(columnName);
                    }
                    column = ObHbaseReaderUtil.buildColumn(value, cell.getColumnType(), encoding, cell.getDateformat(), timezone);
                }
                record.addColumn(column);
            }
        } finally {
            this.columnMap.clear();
            this.versionMap.clear();
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        DBUtil.closeDBResources(rs, stmt, conn);
    }

    private void resetConnection() throws SQLException {
        if (reuseConn && conn != null && !conn.isClosed()) {
            return;
        }
        // set ob_query_timeout and ob_trx_timeout to a large time in case timeout
        int queryTimeoutSeconds = 60 * 60 * 48;
        String setQueryTimeout = "set ob_query_timeout=" + (queryTimeoutSeconds * 1000 * 1000L);
        String setTrxTimeout = "set ob_trx_timeout=" + ((queryTimeoutSeconds + 5) * 1000 * 1000L);
        List<String> newSessionConfig = Lists.newArrayList(setQueryTimeout, setTrxTimeout);
        List<String> sessionConfig = configuration.getList(Key.SESSION, new ArrayList<>(), String.class);
        newSessionConfig.addAll(sessionConfig);
        configuration.set(Key.SESSION, newSessionConfig);
        conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl, this.username, this.password);
    }
}
