package com.alibaba.datax.plugin.writer.oceanbasev10writer.ext;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.util.ObWriterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.Config;
import com.alipay.oceanbase.obproxy.datasource.ObGroupDataSource;
import com.alipay.oceanbase.obproxy.exception.ConnectionPropertiesNotSupportedException;
import com.alipay.oceanbase.obproxy.util.StringParser.IllegalFormatException;
import com.google.common.collect.Maps;

public class OBDataSourceV10 {
	private static final Logger LOG = LoggerFactory.getLogger(OBDataSourceV10.class);

	private static final Map<String, DataSourceHolder> dataSources = Maps.newHashMap();

	private static int ocjGetConnectionTimeout = 0;
	private static int ocjGlobalProxyroGetConnectionTimeout = 0;
	private static int ocjMaxWaitOfCreateClusterResourceMs = 0;

	private static Configuration taskConfig;

	public static String genKey(String fullUserName, String dbName) {
		//username@tenantName#clusterName/dbName
		return fullUserName + "/" + dbName;
	}
	
	public static synchronized void init(Configuration configuration,
			final String fullUsername,
			final String password,
			final String dbName) {
		taskConfig = configuration;
		final String rsUrl = "";
		final String dataSourceKey = genKey(fullUsername, dbName);
		final int maxActiveConnection = configuration.getInt(Config.MAX_ACTIVE_CONNECTION, Config.DEFAULT_MAX_ACTIVE_CONNECTION);
		if (dataSources.containsKey(dataSourceKey)) {
			dataSources.get(dataSourceKey).increseRefercnce();
		} else {
			long timeout = configuration.getInt(Config.TIMEOUT, 30);
			if (timeout < 30) {
				timeout = 30;
			}
			if (ocjGetConnectionTimeout == 0) {
				ocjGetConnectionTimeout = configuration.getInt(Config.OCJ_GET_CONNECT_TIMEOUT,
						Config.DEFAULT_OCJ_GET_CONNECT_TIMEOUT);
				ocjGlobalProxyroGetConnectionTimeout = configuration.getInt(Config.OCJ_PROXY_CONNECT_TIMEOUT,
						Config.DEFAULT_OCJ_PROXY_CONNECT_TIMEOUT);
				ocjMaxWaitOfCreateClusterResourceMs = configuration.getInt(Config.OCJ_CREATE_RESOURCE_TIMEOUT,
						Config.DEFAULT_OCJ_CREATE_RESOURCE_TIMEOUT);

				LOG.info(String.format("initializing OCJ with ocjGetConnectionTimeout=%d, " +
								"ocjGlobalProxyroGetConnectionTimeout=%d, ocjMaxWaitOfCreateClusterResourceMs=%d",
						ocjGetConnectionTimeout, ocjGlobalProxyroGetConnectionTimeout, ocjMaxWaitOfCreateClusterResourceMs));
			}
			DataSourceHolder holder = null;
			try {
				holder = new DataSourceHolder(rsUrl, fullUsername, password, dbName, maxActiveConnection, timeout);
				dataSources.put(dataSourceKey, holder);
			} catch (ConnectionPropertiesNotSupportedException e) {
				e.printStackTrace();
				throw new DataXException(ObDataSourceErrorCode.DESC, "connect error");
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
				throw new DataXException(ObDataSourceErrorCode.DESC, "connect error");
			} catch (IllegalFormatException e) {
				e.printStackTrace();
				throw new DataXException(ObDataSourceErrorCode.DESC, "connect error");
			} catch (SQLException e) {
				e.printStackTrace();
				throw new DataXException(ObDataSourceErrorCode.DESC, "connect error");
			}
		}
	}

	public static synchronized void destory(final String dataSourceKey){
		DataSourceHolder holder = dataSources.get(dataSourceKey);
		holder.decreaseReference();
		if (holder.canClose()) {
			dataSources.remove(dataSourceKey);
			holder.close();
			LOG.info(String.format("close datasource success [%s]", dataSourceKey));
		}
	}
	
	public static Connection getConnection(final String url) {
		Connection conn = null;
		try {
			conn = dataSources.get(url).getconnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

	private static Map<String, String> buildJdbcProperty() {
	    Map<String, String> property = new HashMap<String, String>();
	    property.put("useServerPrepStmts", "false");
	    property.put("characterEncoding", "UTF-8");
	    property.put("useLocalSessionState", "false");
	    property.put("rewriteBatchedStatements", "true");
	    property.put("socketTimeout", "25000");

	    return property;
	}

	private static class DataSourceHolder {
		private volatile int reference;
		private final ObGroupDataSource groupDataSource;
		public static final Map<String, String> jdbcProperty = buildJdbcProperty();;

		public DataSourceHolder(final String rsUrl,
				final String fullUsername,
				final String password,
				final String dbName,
				final int maxActive,
				final long timeout) throws ConnectionPropertiesNotSupportedException, IllegalFormatException, IllegalArgumentException, SQLException {
			this.reference = 1;
			this.groupDataSource = new ObGroupDataSource();
		    this.groupDataSource.setUrl(rsUrl);
		    this.groupDataSource.setFullUsername(fullUsername);
		    this.groupDataSource.setPassword(password);
		    this.groupDataSource.setDatabase(dbName);
		    this.groupDataSource.setConnectionProperties(jdbcProperty);
		    this.groupDataSource.setGetConnectionTimeout(ocjGetConnectionTimeout);
		    this.groupDataSource.setGlobalProxyroGetConnectionTimeout(ocjGlobalProxyroGetConnectionTimeout);
		    this.groupDataSource.setMaxWaitOfCreateClusterResourceMs(ocjMaxWaitOfCreateClusterResourceMs);
		    this.groupDataSource.setMaxActive(maxActive);
		    this.groupDataSource.setGlobalSlowQueryThresholdUs(3000000);      // 3s, sql with response time more than 3s will be logged
		    this.groupDataSource.setGlobalCleanLogFileEnabled(true);          // enable log cleanup
		    this.groupDataSource.setGlobalLogFileSizeThreshold(17179869184L); // 16G, log file total size
		    this.groupDataSource.setGlobalCleanLogFileInterval(10000);        // 10s, check interval
		    this.groupDataSource.setInitialSize(1);

			List<String> initSqls = new ArrayList<String>();
			if (taskConfig != null) {
				List<String> sessionConfig = taskConfig.getList(Key.SESSION, new ArrayList(), String.class);
				if (sessionConfig != null || sessionConfig.size() > 0) {
					initSqls.addAll(sessionConfig);
				}
			}
            // set up for writing timestamp columns
			if (ObWriterUtils.isOracleMode()) {
				initSqls.add("ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS';");
				initSqls.add("ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF';");
				initSqls.add("ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';");
			}

			this.groupDataSource.setConnectionInitSqls(initSqls);

		    this.groupDataSource.init();
			// this.groupDataSource;
		    LOG.info("Create GroupDataSource rsUrl=[{}], fullUserName=[{}], dbName=[{}], getConnectionTimeout= {}ms, maxActive={}",
		    		rsUrl, fullUsername, dbName, 5000, maxActive);
		}
		
		public Connection getconnection() throws SQLException {
			return groupDataSource.getConnection();
		}

		public synchronized void increseRefercnce() {
			this.reference++;
		}

		public synchronized void decreaseReference() {
			this.reference--;
		}

		public synchronized boolean canClose() {
			return reference == 0;
		}

		public synchronized void close() {
			if (this.canClose()) {
				groupDataSource.destroy();
			}
		}
	}

}
