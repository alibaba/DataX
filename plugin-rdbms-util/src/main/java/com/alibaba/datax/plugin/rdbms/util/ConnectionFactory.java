package com.alibaba.datax.plugin.rdbms.util;

import java.sql.Connection;

/**
 * Date: 15/3/16 下午2:17
 */
public interface ConnectionFactory {

    public Connection getConnecttion();

    public Connection getConnecttionWithoutRetry();

    public String getConnectionInfo();

}
