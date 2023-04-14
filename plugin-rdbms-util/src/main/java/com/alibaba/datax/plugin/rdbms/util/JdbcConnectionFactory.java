package com.alibaba.datax.plugin.rdbms.util;

import java.sql.Connection;
import java.util.Properties;

/**
 * Date: 15/3/16 下午3:12
 */
public class JdbcConnectionFactory implements ConnectionFactory {

    private DataBaseType dataBaseType;

    private String jdbcUrl;

    private String userName;

    private String password;

	private Properties prop;

    public JdbcConnectionFactory(DataBaseType dataBaseType, String jdbcUrl, String userName, String password, Properties prop) {
        this.dataBaseType = dataBaseType;
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
		this.prop = prop;
    }

    @Override
    public Connection getConnecttion() {
        return DBUtil.getConnection(dataBaseType, jdbcUrl, userName, password, prop);
    }

    @Override
    public Connection getConnecttionWithoutRetry() {
        return DBUtil.getConnectionWithoutRetry(dataBaseType, jdbcUrl, userName, password, prop);
    }

    @Override
    public String getConnectionInfo() {
        return "jdbcUrl:" + jdbcUrl;
    }
}
