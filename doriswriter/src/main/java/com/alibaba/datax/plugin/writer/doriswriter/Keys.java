package com.alibaba.datax.plugin.writer.doriswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Keys implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final int MAX_RETRIES = 3;
    private static final int BATCH_ROWS = 100000;
    private static final long DEFAULT_FLUSH_INTERVAL = 10000;
    private static final long DEFAULT_MAX_BATCH_SIZE = 50L * 1024 * 1024;
    private static final int DEFAULT_FLUSH_QUEUE_LENGTH = 4;
    private static final int DEFAULT_CONNECT_TIMEOUT = 5000;
    private static final int DEFAULT_SOCKET_TIMEOUT = 600000;
    private static final int DEFAULT_CONNECTION_REQUEST_TIMEOUT = 5000;
    private static final long DEFAULT_HOST_COOLDOWN_MS = 30000L;
    private static final int DEFAULT_INITIAL_BUFFER_SIZE = 1024 * 1024;
    private static final int DEFAULT_FLUSH_WORKER_COUNT = 1;

    private static final String LOAD_PROPS_FORMAT = "format";
    private static final String LOAD_PROPS_LINE_DELIMITER = "line_delimiter";
    private static final String LOAD_PROPS_COLUMN_SEPARATOR = "column_separator";

    public enum StreamLoadFormat {
        CSV, JSON;
    }

    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String DATABASE = "connection[0].selectedDatabase";
    private static final String TABLE = "connection[0].table[0]";
    private static final String COLUMN = "column";
    private static final String PRE_SQL = "preSql";
    private static final String POST_SQL = "postSql";
    private static final String JDBC_URL = "connection[0].jdbcUrl";
    private static final String LABEL_PREFIX = "labelPrefix";
    private static final String MAX_BATCH_ROWS = "maxBatchRows";
    private static final String MAX_BATCH_SIZE = "batchSize";
    private static final String FLUSH_INTERVAL = "flushInterval";
    private static final String LOAD_URL = "loadUrl";
    private static final String FLUSH_QUEUE_LENGTH = "flushQueueLength";
    private static final String LOAD_PROPS = "loadProps";
    private static final String MAX_RETRY_COUNT = "maxRetries";
    private static final String CONNECT_TIMEOUT = "connectTimeout";
    private static final String SOCKET_TIMEOUT = "socketTimeout";
    private static final String CONNECTION_REQUEST_TIMEOUT = "connectionRequestTimeout";
    private static final String HOST_COOLDOWN_MS = "hostCooldownMs";
    private static final String INITIAL_BUFFER_SIZE = "initialBufferSize";
    private static final String FLUSH_WORKER_COUNT = "flushWorkerCount";

    private static final String DEFAULT_LABEL_PREFIX = "datax_doris_writer_";

    private final Configuration options;

    private List<String> infoSchemaColumns;
    private final List<String> userSetColumns;
    private final boolean isWildcardColumn;

    public Keys(Configuration options) {
        this.options = options;
        List<String> configuredColumns = options.getList(COLUMN, String.class);
        this.userSetColumns = configuredColumns == null ? Collections.<String>emptyList() : configuredColumns.stream().map(str -> str.replace("`", "")).collect(Collectors.toList());
        this.isWildcardColumn = configuredColumns != null && configuredColumns.size() == 1 && "*".equals(configuredColumns.get(0).trim());
    }

    public void doPretreatment() {
        validateRequired();
        validateStreamLoadUrl();
    }

    public String getJdbcUrl() {
        return options.getString(JDBC_URL);
    }

    public String getDatabase() {
        return options.getString(DATABASE);
    }

    public String getTable() {
        return options.getString(TABLE);
    }

    public String getUsername() {
        return options.getString(USERNAME);
    }

    public String getPassword() {
        return options.getString(PASSWORD);
    }

    public String getLabelPrefix() {
        String label = options.getString(LABEL_PREFIX);
        return label == null ? DEFAULT_LABEL_PREFIX : label;
    }

    public List<String> getLoadUrlList() {
        return options.getList(LOAD_URL, String.class);
    }

    public List<String> getColumns() {
        return isWildcardColumn ? this.infoSchemaColumns : this.userSetColumns;
    }

    public boolean isWildcardColumn() {
        return this.isWildcardColumn;
    }

    public void setInfoCchemaColumns(List<String> cols) {
        this.infoSchemaColumns = cols;
    }

    public List<String> getPreSqlList() {
        return options.getList(PRE_SQL, String.class);
    }

    public List<String> getPostSqlList() {
        return options.getList(POST_SQL, String.class);
    }

    public Map<String, Object> getLoadProps() {
        return options.getMap(LOAD_PROPS);
    }

    public int getMaxRetries() {
        Integer retries = options.getInt(MAX_RETRY_COUNT);
        return retries == null ? MAX_RETRIES : retries;
    }

    public int getBatchRows() {
        Integer rows = options.getInt(MAX_BATCH_ROWS);
        return rows == null ? BATCH_ROWS : rows;
    }

    public long getBatchSize() {
        Long size = options.getLong(MAX_BATCH_SIZE);
        return size == null ? DEFAULT_MAX_BATCH_SIZE : size;
    }

    public long getFlushInterval() {
        Long interval = options.getLong(FLUSH_INTERVAL);
        return interval == null ? DEFAULT_FLUSH_INTERVAL : interval;
    }

    public int getFlushQueueLength() {
        Integer len = options.getInt(FLUSH_QUEUE_LENGTH);
        return len == null ? DEFAULT_FLUSH_QUEUE_LENGTH : len;
    }

    public int getConnectTimeout() {
        Integer timeout = options.getInt(CONNECT_TIMEOUT);
        return timeout == null ? DEFAULT_CONNECT_TIMEOUT : timeout;
    }

    public int getSocketTimeout() {
        Integer timeout = options.getInt(SOCKET_TIMEOUT);
        return timeout == null ? DEFAULT_SOCKET_TIMEOUT : timeout;
    }

    public int getConnectionRequestTimeout() {
        Integer timeout = options.getInt(CONNECTION_REQUEST_TIMEOUT);
        return timeout == null ? DEFAULT_CONNECTION_REQUEST_TIMEOUT : timeout;
    }

    public long getHostCooldownMs() {
        Long value = options.getLong(HOST_COOLDOWN_MS);
        return value == null ? DEFAULT_HOST_COOLDOWN_MS : value;
    }

    public int getInitialBufferSize() {
        Integer size = options.getInt(INITIAL_BUFFER_SIZE);
        return size == null ? DEFAULT_INITIAL_BUFFER_SIZE : size;
    }

    public int getFlushWorkerCount() {
        Integer count = options.getInt(FLUSH_WORKER_COUNT);
        return count == null ? DEFAULT_FLUSH_WORKER_COUNT : Math.max(1, count);
    }

    public StreamLoadFormat getStreamLoadFormat() {
        Map<String, Object> loadProps = getLoadProps();
        if (loadProps == null) {
            return StreamLoadFormat.CSV;
        }
        if (loadProps.containsKey(LOAD_PROPS_FORMAT)
                && StreamLoadFormat.JSON.name().equalsIgnoreCase(String.valueOf(loadProps.get(LOAD_PROPS_FORMAT)))) {
            return StreamLoadFormat.JSON;
        }
        return StreamLoadFormat.CSV;
    }

    public String getLineDelimiter() {
        Map<String, Object> loadProps = getLoadProps();
        Object value = loadProps == null ? null : loadProps.get(LOAD_PROPS_LINE_DELIMITER);
        return value == null ? "\n" : String.valueOf(value);
    }

    public String getColumnSeparator() {
        Map<String, Object> loadProps = getLoadProps();
        Object value = loadProps == null ? null : loadProps.get(LOAD_PROPS_COLUMN_SEPARATOR);
        return value == null ? "\t" : String.valueOf(value);
    }

    private void validateStreamLoadUrl() {
        List<String> urlList = getLoadUrlList();
        for (String host : urlList) {
            if (host.split(":").length < 2) {
                throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                        "The format of loadUrl is not correct, please enter:[`fe_ip:fe_http_ip;fe_ip:fe_http_ip`].");
            }
        }
    }

    private void validateRequired() {
        final String[] requiredOptionKeys = new String[]{
                USERNAME,
                DATABASE,
                TABLE,
                COLUMN,
                LOAD_URL
        };
        for (String optionKey : requiredOptionKeys) {
            options.getNecessaryValue(optionKey, DBUtilErrorCode.REQUIRED_VALUE);
        }
    }
}
