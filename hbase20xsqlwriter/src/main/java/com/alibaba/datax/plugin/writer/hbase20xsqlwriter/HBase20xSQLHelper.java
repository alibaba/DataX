package com.alibaba.datax.plugin.writer.hbase20xsqlwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class HBase20xSQLHelper {
    private static final Logger LOG = LoggerFactory.getLogger(HBase20xSQLHelper.class);

    /**
     * phoenix瘦客户端连接前缀
     */
    public static final String CONNECT_STRING_PREFIX = "jdbc:phoenix:thin:";
    /**
     * phoenix驱动名
     */
    public static final String CONNECT_DRIVER_STRING = "org.apache.phoenix.queryserver.client.Driver";
    /**
     * 从系统表查找配置表信息
     */
    public static final String SELECT_CATALOG_TABLE_STRING = "SELECT COLUMN_NAME FROM SYSTEM.CATALOG WHERE TABLE_NAME='%s' AND COLUMN_NAME IS NOT NULL";

    /**
     * 验证配置参数是否正确
     */
    public static void validateParameter(com.alibaba.datax.common.util.Configuration originalConfig) {
        // 表名和queryserver地址必须配置，否则抛异常
        String tableName = originalConfig.getNecessaryValue(Key.TABLE, HBase20xSQLWriterErrorCode.REQUIRED_VALUE);
        String queryServerAddress = originalConfig.getNecessaryValue(Key.QUERYSERVER_ADDRESS, HBase20xSQLWriterErrorCode.REQUIRED_VALUE);

        // 序列化格式，可不配置，默认PROTOBUF
        String serialization = originalConfig.getString(Key.SERIALIZATION_NAME, Constant.DEFAULT_SERIALIZATION);

        String connStr = getConnectionUrl(queryServerAddress, serialization);
        // 校验jdbc连接是否正常
        Connection conn = getThinClientConnection(connStr);

        List<String> columnNames = originalConfig.getList(Key.COLUMN, String.class);
        if (columnNames == null || columnNames.isEmpty()) {
            throw DataXException.asDataXException(
                    HBase20xSQLWriterErrorCode.ILLEGAL_VALUE, "HBase的columns配置不能为空,请添加目标表的列名配置.");
        }
        String schema = originalConfig.getString(Key.SCHEMA);
        // 检查表以及配置列是否存在
        checkTable(conn, schema, tableName, columnNames);
    }

    /**
     * 获取JDBC连接，轻量级连接，使用完后必须显式close
     */
    public static Connection getThinClientConnection(String connStr) {
        LOG.debug("Connecting to QueryServer [" + connStr + "] ...");
        Connection conn;
        try {
            Class.forName(CONNECT_DRIVER_STRING);
            conn = DriverManager.getConnection(connStr);
            conn.setAutoCommit(false);
        } catch (Throwable e) {
            throw DataXException.asDataXException(HBase20xSQLWriterErrorCode.GET_QUERYSERVER_CONNECTION_ERROR,
                    "无法连接QueryServer，配置不正确或服务未启动，请检查配置和服务状态或者联系HBase管理员.", e);
        }
        LOG.debug("Connected to QueryServer successfully.");
        return conn;
    }

    public static Connection getJdbcConnection(Configuration conf) {
        String queryServerAddress = conf.getNecessaryValue(Key.QUERYSERVER_ADDRESS, HBase20xSQLWriterErrorCode.REQUIRED_VALUE);
        // 序列化格式，可不配置，默认PROTOBUF
        String serialization = conf.getString(Key.SERIALIZATION_NAME, "PROTOBUF");
        String connStr = getConnectionUrl(queryServerAddress, serialization);
        return getThinClientConnection(connStr);
    }


    public static String getConnectionUrl(String queryServerAddress, String serialization) {
        String urlFmt = CONNECT_STRING_PREFIX + "url=%s;serialization=%s";
        return String.format(urlFmt, queryServerAddress, serialization);
    }

    public static void checkTable(Connection conn, String schema, String tableName, List<String> columnNames) throws DataXException {
        String selectSystemTable = getSelectSystemSQL(schema, tableName);
        Statement st = null;
        ResultSet rs = null;
        try {
            st = conn.createStatement();
            rs = st.executeQuery(selectSystemTable);
            List<String> allColumns = new ArrayList<String>();
            if (rs.next()) {
                allColumns.add(rs.getString(1));
            } else {
                LOG.error(tableName + "表不存在，请检查表名是否正确或是否已创建.", HBase20xSQLWriterErrorCode.GET_HBASE_TABLE_ERROR);
                throw DataXException.asDataXException(HBase20xSQLWriterErrorCode.GET_HBASE_TABLE_ERROR,
                        tableName + "表不存在，请检查表名是否正确或是否已创建.");
            }
            while (rs.next()) {
                allColumns.add(rs.getString(1));
            }
            for (String columnName : columnNames) {
                if (!allColumns.contains(columnName)) {
                    // 用户配置的列名在元数据中不存在
                    throw DataXException.asDataXException(HBase20xSQLWriterErrorCode.ILLEGAL_VALUE,
                            "您配置的列" + columnName + "在目的表" + tableName + "的元数据中不存在，请检查您的配置或者联系HBase管理员.");
                }
            }

        } catch (SQLException t) {
            throw DataXException.asDataXException(HBase20xSQLWriterErrorCode.GET_HBASE_TABLE_ERROR,
                    "获取表" + tableName + "信息失败，请检查您的集群和表状态或者联系HBase管理员.", t);
        } finally {
            closeJdbc(conn, st, rs);
        }
    }

    private static String getSelectSystemSQL(String schema, String tableName) {
        String sql = String.format(SELECT_CATALOG_TABLE_STRING, tableName);
        if (schema != null) {
            sql = sql + " AND TABLE_SCHEM = '" + schema + "'";
        }
        return sql;
    }

    public static void closeJdbc(Connection connection, Statement statement, ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            LOG.warn("数据库连接关闭异常.", HBase20xSQLWriterErrorCode.CLOSE_HBASE_CONNECTION_ERROR);
        }
    }
}
