package com.alibaba.datax.plugin.writer.oceanbasev10writer.ext;

import java.sql.Connection;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DirectPathAbstractConnHolder {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractConnHolder.class);
    protected Configuration config;
    protected String jdbcUrl;
    protected String userName;
    protected String password;

    protected Connection conn;

    protected DirectPathAbstractConnHolder(Configuration config, String jdbcUrl, String userName, String password) {
        this.config = config;
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
    }

    public Connection reconnect() {
        DBUtil.closeDBResources(null, conn);
        return initConnection();
    }

    public Connection getConn() {
        if (conn == null) {
            return initConnection();
        } else {
            try {
                if (conn.isClosed()) {
                    return reconnect();
                }
                return conn;
            } catch (Exception e) {
                LOG.debug("can not judge whether the hold connection is closed or not, just reuse the hold connection");
                return conn;
            }
        }
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public Configuration getConfig() {
        return config;
    }

    public void doCommit() {}

    public abstract void destroy();

    public abstract Connection initConnection();
}
