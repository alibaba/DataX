package com.alibaba.datax.plugin.writer.oceanbasev10writer.ext;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ServerConnectInfo {
	
	public String clusterName;
	public String tenantName;
	public String userName;
	public String password;
	public String databaseName;
	public String ipPort;
	public String jdbcUrl;

	public ServerConnectInfo(final String jdbcUrl, final String username, final String password) {
		if (jdbcUrl.startsWith(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING)) {
			String[] ss = jdbcUrl.split(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING_PATTERN);
			if (ss.length != 3) {
				throw new RuntimeException("jdbc url format is not correct: " + jdbcUrl);
			}
			this.userName = username;
			this.clusterName = ss[1].trim().split(":")[0];
			this.tenantName  = ss[1].trim().split(":")[1];
			this.jdbcUrl = ss[2].replace("jdbc:mysql:", "jdbc:oceanbase:");
		} else {
			throw new RuntimeException ("jdbc url format is not correct: " + jdbcUrl);
		}
		this.password = password;
		parseJdbcUrl(jdbcUrl);
	}

	private void parseJdbcUrl(final String jdbcUrl) {
		Pattern pattern = Pattern.compile("//([\\w\\.\\-]+:\\d+)/([\\w]+)\\?");
		Matcher matcher = pattern.matcher(jdbcUrl);
		if (matcher.find()) {
			String ipPort = matcher.group(1);
			String dbName = matcher.group(2);
			this.ipPort = ipPort;
			this.databaseName = dbName;
		} else {
			throw new RuntimeException("Invalid argument:" + jdbcUrl);
		}
	}

	public String toString() {
		StringBuffer strBuffer = new StringBuffer();
		return strBuffer.append("clusterName:").append(clusterName).append(", tenantName:").append(tenantName)
				.append(", userName:").append(userName).append(", databaseName:").append(databaseName)
				.append(", ipPort:").append(ipPort).append(", jdbcUrl:").append(jdbcUrl).toString();
	}

	public String getFullUserName() {
		StringBuilder builder = new StringBuilder();
		builder.append(userName).append("@").append(tenantName).append("#").append(clusterName);
		return builder.toString();
	}
}
