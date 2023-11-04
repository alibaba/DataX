package com.alibaba.datax.plugin.writer.oceanbasev10writer.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.Config;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbUtils {

    protected static final Logger LOG = LoggerFactory.getLogger(DbUtils.class);

    public static String fetchSingleValueWithRetry(Configuration config, String query) {
        final String username = config.getString(Key.USERNAME);
        final String password = config.getString(Key.PASSWORD);
        String jdbcUrl = config.getString(Key.JDBC_URL);

        if (jdbcUrl == null) {
            List<Object> conns = config.getList(Constant.CONN_MARK, Object.class);
            Configuration connConf = Configuration.from(conns.get(0).toString());
            jdbcUrl = connConf.getString(Key.JDBC_URL);
        }

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet result = null;
        String value = null;
        int retry = 0;
        int failTryCount = config.getInt(Config.FAIL_TRY_COUNT, Config.DEFAULT_FAIL_TRY_COUNT);
        do {
            try {
                if (retry > 0) {
                    int sleep = retry > 9 ? 500 : 1 << retry;
                    try {
                        TimeUnit.SECONDS.sleep(sleep);
                    } catch (InterruptedException e) {
                    }
                    LOG.warn("retry fetch value for {} the {} times", query, retry);
                }
                conn = DBUtil.getConnection(DataBaseType.OceanBase, jdbcUrl, username, password);
                stmt = conn.prepareStatement(query);
                result = stmt.executeQuery();
                if (result.next()) {
                    value = result.getString("Value");
                } else {
                    throw new RuntimeException("no values returned for " + query);
                }
                LOG.info("value for query [{}] is [{}]", query, value);
                break;
            } catch (SQLException e) {
                ++retry;
                LOG.warn("fetch value with {} error {}", query, e);
            } finally {
                DBUtil.closeDBResources(result, stmt, conn);
            }
        } while (retry < failTryCount);

        return value;
    }

    /**
     * build sys connection from ordinary jdbc url
     *
     * @param jdbcUrl
     * @param clusterName
     * @return
     * @throws Exception
     */
    public static Connection buildSysConn(String jdbcUrl, String clusterName) throws Exception {
        jdbcUrl = jdbcUrl.replace("jdbc:mysql://", "jdbc:oceanbase://");
        int startIdx = jdbcUrl.indexOf('/', "jdbc:oceanbase://".length());
        int endIdx = jdbcUrl.lastIndexOf('?');
        String prefix = jdbcUrl.substring(0, startIdx + 1);
        final String postfix = jdbcUrl.substring(endIdx);
        String sysJDBCUrl = prefix + "oceanbase" + postfix;

        String tenantName = "sys";
        String[][] userConfigs = {
            {"monitor", "monitor"}
        };

        Connection conn = null;
        for (String[] userConfig : userConfigs) {
            try {
                conn = DBUtil.getConnectionWithoutRetry(DataBaseType.OceanBase, sysJDBCUrl, String.format("%s@%s#%s", userConfig[0],
                    tenantName, clusterName), userConfig[1]);
            } catch (Exception e) {
                LOG.warn("fail connecting to ob: " + e.getMessage());

            }
            if (conn == null) {
                LOG.warn("fail to get connection with user " + userConfig[0] + ", try alternative user.");
            } else {
                break;
            }
        }

        if (conn == null) {
            throw new Exception("fail to get connection with sys tenant.");
        }

        return conn;
    }
}
