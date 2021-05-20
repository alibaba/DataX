package com.alibaba.datax.plugin.writer.oceanbasev10writer.ext;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;

import java.sql.Connection;

public abstract class ConnHolder {

    protected final Configuration config;
    protected Connection conn;

    public ConnHolder(Configuration config) {
        this.config = config;
    }

    public abstract Connection initConnection();

    public Configuration getConfig() {
        return config;
    }

    public Connection getConn() {
        return conn;
    }

    public Connection reconnect() {
        DBUtil.closeDBResources(null, conn);
        return initConnection();
    }

    public abstract String getJdbcUrl();

    public abstract String getUserName();

    public abstract void destroy();
}
