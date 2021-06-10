package com.alibaba.datax.plugin.writer.doriswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class Key implements Serializable
{
    private final Configuration options;

    public Key(final Configuration options) {
        this.options = options;
    }

    public void doPretreatment() {
        this.validateRequired();
        this.validateStreamLoadUrl();
    }

    public String getJdbcUrl() {
        return this.options.getString("jdbcUrl");
    }

    public String getDatabase() {
        return this.options.getString("database");
    }

    public String getTable() {
        return this.options.getString("table");
    }

    public String getUsername() {
        return this.options.getString("username");
    }

    public String getPassword() {
        return this.options.getString("password");
    }

    public List<String> getLoadUrlList() {
        return this.options.getList("loadUrl", String.class);
    }

    public List<String> getColumns() {
        return this.options.getList("column", String.class);
    }

    public List<String> getPreSqlList() {
        return this.options.getList("preSql", String.class);
    }

    public List<String> getPostSqlList() {
        return this.options.getList("postSql", String.class);
    }

    public Map<String, Object> getLoadProps() {
        return this.options.getMap("loadProps");
    }

    public int getMaxRetries() {
        return 1;
    }

    public int getBatchRows() {
        final Integer rows = this.options.getInt("maxBatchRows");
        return (null == rows) ? 500000 : rows;
    }

    public long getBatchSize() {
        final Long size = this.options.getLong("maxBatchSize");
        return (null == size) ? 94371840L : size;
    }

    public int getFlushQueueLength() {
        final Integer len = this.options.getInt("flushQueueLength");
        return (null == len) ? 1 : len;
    }

    private void validateStreamLoadUrl() {
        final List<String> urlList = this.getLoadUrlList();
        for (final String host : urlList) {
            if (host.split(":").length < 2) {
                throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR, "loadUrl的格式不正确，请输入 `fe_ip:fe_http_ip;fe_ip:fe_http_ip`。");
            }
        }
    }

    private void validateRequired() {
        final String[] requiredOptionKeys =  new String[] { "username", "password", "database", "table", "column", "loadUrl" };
        for (final String optionKey : requiredOptionKeys) {
            this.options.getNecessaryValue(optionKey, DBUtilErrorCode.REQUIRED_VALUE);
        }
    }


}