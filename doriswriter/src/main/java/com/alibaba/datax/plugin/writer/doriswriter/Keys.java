package com.alibaba.datax.plugin.writer.doriswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Keys implements Serializable {

    private static final long serialVersionUID = 1l;
    private static final int MAX_RETRIES = 3;
    private static final int BATCH_ROWS = 500000;
    private static final long DEFAULT_FLUSH_INTERVAL = 30000;

    private static final String LOAD_PROPS_FORMAT = "format";
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

    private static final String DEFAULT_LABEL_PREFIX = "datax_doris_writer_";

    private static final long DEFAULT_MAX_BATCH_SIZE = 90 * 1024 * 1024; //default 90M

    private final Configuration options;

    private List<String> infoSchemaColumns;
    private List<String> userSetColumns;
    private boolean isWildcardColumn;

    public Keys ( Configuration options) {
        this.options = options;
        this.userSetColumns = options.getList(COLUMN, String.class).stream().map(str -> str.replace("`", "")).collect(Collectors.toList());
        if (1 == options.getList(COLUMN, String.class).size() && "*".trim().equals(options.getList(COLUMN, String.class).get(0))) {
            this.isWildcardColumn = true;
        }
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
        return null == label ? DEFAULT_LABEL_PREFIX : label;
    }

    public List<String> getLoadUrlList() {
        return options.getList(LOAD_URL, String.class);
    }

    public List<String> getColumns() {
        if (isWildcardColumn) {
            return this.infoSchemaColumns;
        }
        return this.userSetColumns;
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
        return MAX_RETRIES;
    }

    public int getBatchRows() {
        Integer rows = options.getInt(MAX_BATCH_ROWS);
        return null == rows ? BATCH_ROWS : rows;
    }

    public long getBatchSize() {
        Long size = options.getLong(MAX_BATCH_SIZE);
        return null == size ? DEFAULT_MAX_BATCH_SIZE : size;
    }

    public long getFlushInterval() {
        Long interval = options.getLong(FLUSH_INTERVAL);
        return null == interval ? DEFAULT_FLUSH_INTERVAL : interval;
    }

    public int getFlushQueueLength() {
        Integer len = options.getInt(FLUSH_QUEUE_LENGTH);
        return null == len ? 1 : len;
    }

    public StreamLoadFormat getStreamLoadFormat() {
        Map<String, Object> loadProps = getLoadProps();
        if (null == loadProps) {
            return StreamLoadFormat.CSV;
        }
        if (loadProps.containsKey(LOAD_PROPS_FORMAT)
                && StreamLoadFormat.JSON.name().equalsIgnoreCase(String.valueOf(loadProps.get(LOAD_PROPS_FORMAT)))) {
            return StreamLoadFormat.JSON;
        }
        return StreamLoadFormat.CSV;
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
