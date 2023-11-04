package com.alibaba.datax.plugin.writer.oceanbasev10writer.ext;

import static org.apache.commons.lang3.StringUtils.EMPTY;

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
	public boolean publicCloud;

	/**
	 *
	 * @param jdbcUrl format is jdbc:oceanbase//ip:port
	 * @param username format is cluster:tenant:username or username@tenant#cluster or user@tenant or user
	 * @param password
	 */
	public ServerConnectInfo(final String jdbcUrl, final String username, final String password) {
		this.jdbcUrl = jdbcUrl;
		this.password = password;
		parseJdbcUrl(jdbcUrl);
		parseFullUserName(username);
	}

	private void parseJdbcUrl(final String jdbcUrl) {
		Pattern pattern = Pattern.compile("//([\\w\\.\\-]+:\\d+)/([\\w-]+)\\?");
		Matcher matcher = pattern.matcher(jdbcUrl);
		if (matcher.find()) {
			String ipPort = matcher.group(1);
			String dbName = matcher.group(2);
			this.ipPort = ipPort;
			this.databaseName = dbName;
			this.publicCloud = ipPort.split(":")[0].endsWith("aliyuncs.com");
		} else {
			throw new RuntimeException("Invalid argument:" + jdbcUrl);
		}
	}

	private void parseFullUserName(final String fullUserName) {
		int tenantIndex = fullUserName.indexOf("@");
		int clusterIndex = fullUserName.indexOf("#");
		if (fullUserName.contains(":") && tenantIndex < 0) {
			String[] names = fullUserName.split(":");
			if (names.length != 3) {
				throw new RuntimeException("invalid argument: " + fullUserName);
			} else {
				this.clusterName = names[0];
				this.tenantName = names[1];
				this.userName = names[2];
			}
		} else if (!publicCloud || tenantIndex < 0) {
			this.userName = tenantIndex < 0 ? fullUserName : fullUserName.substring(0, tenantIndex);
			this.clusterName = clusterIndex < 0 ? EMPTY : fullUserName.substring(clusterIndex + 1);
			this.tenantName = tenantIndex < 0 ? EMPTY : fullUserName.substring(tenantIndex + 1, clusterIndex);
		} else {
			// If in public cloud, the username with format user@tenant#cluster should be parsed, otherwise, connection can't be created.
			this.userName = fullUserName.substring(0, tenantIndex);
			if (clusterIndex > tenantIndex) {
				this.tenantName = fullUserName.substring(tenantIndex + 1, clusterIndex);
				this.clusterName = fullUserName.substring(clusterIndex + 1);
			} else {
				this.tenantName = fullUserName.substring(tenantIndex + 1);
				this.clusterName = EMPTY;
			}
		}
	}

	@Override
	public String toString() {
		StringBuffer strBuffer = new StringBuffer();
		return strBuffer.append("clusterName:").append(clusterName).append(", tenantName:").append(tenantName)
				.append(", userName:").append(userName).append(", databaseName:").append(databaseName)
				.append(", ipPort:").append(ipPort).append(", jdbcUrl:").append(jdbcUrl).toString();
	}

	public String getFullUserName() {
		StringBuilder builder = new StringBuilder();
		builder.append(userName);
		if (!EMPTY.equals(tenantName)) {
			builder.append("@").append(tenantName);
		}

		if (!EMPTY.equals(clusterName)) {
			builder.append("#").append(clusterName);
		}
		if (EMPTY.equals(this.clusterName) && EMPTY.equals(this.tenantName)) {
			return this.userName;
		}
		return builder.toString();
	}
}
