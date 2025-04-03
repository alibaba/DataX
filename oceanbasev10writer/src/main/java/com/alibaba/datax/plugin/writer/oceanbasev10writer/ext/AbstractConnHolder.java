package com.alibaba.datax.plugin.writer.oceanbasev10writer.ext;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

public abstract class AbstractConnHolder {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractConnHolder.class);

    protected final Configuration config;
    protected Connection conn;

    protected String jdbcUrl;
    protected String userName;
    protected String password;

    protected AbstractConnHolder(Configuration config, String jdbcUrl, String userName, String password) {
        this.config = config;
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
    }

    public AbstractConnHolder(Configuration config) {
        this.config = config;
    }

    public abstract Connection initConnection();

    public Configuration getConfig() {
        return config;
    }

    public Connection getConn() {
        try {
            if (conn != null && !conn.isClosed()) {
                return conn;
            }
        } catch (Exception e) {
            LOG.warn("judge connection is closed or not failed. try to reconnect.", e);
        }
        return reconnect();
    }

    public Connection reconnect() {
        DBUtil.closeDBResources(null, conn);
        return initConnection();
    }

    public abstract String getJdbcUrl();

    public abstract String getUserName();

    public abstract void destroy();

    public abstract void doCommit();
}
