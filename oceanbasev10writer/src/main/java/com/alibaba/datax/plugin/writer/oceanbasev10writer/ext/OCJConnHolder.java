package com.alibaba.datax.plugin.writer.oceanbasev10writer.ext;

import java.sql.Connection;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;

/**
 * wrap oceanbase java client
 * @author oceanbase
 */

public class OCJConnHolder extends ConnHolder {
	private ServerConnectInfo connectInfo;
	private String dataSourceKey;

	public OCJConnHolder (Configuration config, ServerConnectInfo connInfo) {
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
	public Connection reconnect() {
		DBUtil.closeDBResources(null, conn);
		return initConnection();
	}

	@Override
	public Connection getConn() {
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
}
