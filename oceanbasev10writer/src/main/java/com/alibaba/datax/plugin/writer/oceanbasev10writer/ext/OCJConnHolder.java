package com.alibaba.datax.plugin.writer.oceanbasev10writer.ext;

import java.sql.Connection;
import java.sql.SQLException;

import com.alibaba.datax.common.util.Configuration;

/**
 * wrap oceanbase java client
 *
 * @author oceanbase
 */

public class OCJConnHolder extends AbstractConnHolder {
    private ServerConnectInfo connectInfo;
    private String dataSourceKey;

    public OCJConnHolder(Configuration config, ServerConnectInfo connInfo) {
        super(config);
        this.connectInfo = connInfo;
        this.dataSourceKey = OBDataSourceV10.genKey(connectInfo.getFullUserName(), connectInfo.databaseName);
        OBDataSourceV10.init(config, connectInfo.getFullUserName(), connectInfo.password, connectInfo.databaseName);
    }

    @Override
    public Connection initConnection() {
        conn = OBDataSourceV10.getConnection(dataSourceKey);
        return conn;
    }

    @Override
    public String getJdbcUrl() {
        return connectInfo.jdbcUrl;
    }

    @Override
    public String getUserName() {
        return connectInfo.userName;
    }

    public void destroy() {
        OBDataSourceV10.destory(this.dataSourceKey);
    }

    public void doCommit() {
        try {
            if (conn != null) {
                conn.commit();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
