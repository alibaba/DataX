package com.alibaba.datax.plugin.rdbms.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public final class DBUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DBUtil.class);

    private static final ThreadLocal<ExecutorService> rsExecutors = new ThreadLocal<ExecutorService>() {
        @Override
        protected ExecutorService initialValue() {
            return Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
                    .setNameFormat("rsExecutors-%d")
                    .setDaemon(true)
                    .build());
        }
    };

    private DBUtil() {
    }

    public static String chooseJdbcUrl(final DataBaseType dataBaseType,
                                       final List<String> jdbcUrls, final String username,
                                       final String password, final List<String> preSql,
                                       final boolean checkSlave) {

        if (null == jdbcUrls || jdbcUrls.isEmpty()) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.CONF_ERROR,
                    String.format("您的jdbcUrl的配置信息有错, 因为jdbcUrl[%s]不能为空. 请检查您的配置并作出修改.",
                            StringUtils.join(jdbcUrls, ",")));
        }

        try {
            return RetryUtil.executeWithRetry(new Callable<String>() {

                @Override
                public String call() throws Exception {
                    boolean connOK = false;
                    for (String url : jdbcUrls) {
                        if (StringUtils.isNotBlank(url)) {
                            url = url.trim();
                            if (null != preSql && !preSql.isEmpty()) {
                                connOK = testConnWithoutRetry(dataBaseType,
                                        url, username, password, preSql);
                            } else {
                                connOK = testConnWithoutRetry(dataBaseType,
                                        url, username, password, checkSlave);
                            }
                            if (connOK) {
                                return url;
                            }
                        }
                    }
                    throw new Exception("DataX无法连接对应的数据库，可能原因是：1) 配置的ip/port/database/jdbc错误，无法连接。2) 配置的username/password错误，鉴权失败。请和DBA确认该数据库的连接信息是否正确。");
//                    throw new Exception(DBUtilErrorCode.JDBC_NULL.toString());
                }
            }, 7, 1000L, true);
            //warn: 7 means 2 minutes
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.CONN_DB_ERROR,
                    String.format("数据库连接失败. 因为根据您配置的连接信息,无法从:%s 中找到可连接的jdbcUrl. 请检查您的配置并作出修改.",
                            StringUtils.join(jdbcUrls, ",")), e);
        }
    }

    public static String chooseJdbcUrlWithoutRetry(final DataBaseType dataBaseType,
                                       final List<String> jdbcUrls, final String username,
                                       final String password, final List<String> preSql,
                                       final boolean checkSlave) throws DataXException {

        if (null == jdbcUrls || jdbcUrls.isEmpty()) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.CONF_ERROR,
                    String.format("您的jdbcUrl的配置信息有错, 因为jdbcUrl[%s]不能为空. 请检查您的配置并作出修改.",
                            StringUtils.join(jdbcUrls, ",")));
        }

        boolean connOK = false;
        for (String url : jdbcUrls) {
            if (StringUtils.isNotBlank(url)) {
                url = url.trim();
                if (null != preSql && !preSql.isEmpty()) {
                    connOK = testConnWithoutRetry(dataBaseType,
                            url, username, password, preSql);
                } else {
                    try {
                        connOK = testConnWithoutRetry(dataBaseType,
                                url, username, password, checkSlave);
                    } catch (Exception e) {
                        throw DataXException.asDataXException(
                                DBUtilErrorCode.CONN_DB_ERROR,
                                String.format("数据库连接失败. 因为根据您配置的连接信息,无法从:%s 中找到可连接的jdbcUrl. 请检查您的配置并作出修改.",
                                        StringUtils.join(jdbcUrls, ",")), e);
                    }
                }
                if (connOK) {
                    return url;
                }
            }
        }
        throw DataXException.asDataXException(
                DBUtilErrorCode.CONN_DB_ERROR,
                String.format("数据库连接失败. 因为根据您配置的连接信息,无法从:%s 中找到可连接的jdbcUrl. 请检查您的配置并作出修改.",
                        StringUtils.join(jdbcUrls, ",")));
    }

    /**
     * 检查slave的库中的数据是否已到凌晨00:00
     * 如果slave同步的数据还未到00:00返回false
     * 否则范围true
     *
     * @author ZiChi
     * @version 1.0 2014-12-01
     */
    private static boolean isSlaveBehind(Connection conn) {
        try {
            ResultSet rs = query(conn, "SHOW VARIABLES LIKE 'read_only'");
            if (DBUtil.asyncResultSetNext(rs)) {
                String readOnly = rs.getString("Value");
                if ("ON".equalsIgnoreCase(readOnly)) { //备库
                    ResultSet rs1 = query(conn, "SHOW SLAVE STATUS");
                    if (DBUtil.asyncResultSetNext(rs1)) {
                        String ioRunning = rs1.getString("Slave_IO_Running");
                        String sqlRunning = rs1.getString("Slave_SQL_Running");
                        long secondsBehindMaster = rs1.getLong("Seconds_Behind_Master");
                        if ("Yes".equalsIgnoreCase(ioRunning) && "Yes".equalsIgnoreCase(sqlRunning)) {
                            ResultSet rs2 = query(conn, "SELECT TIMESTAMPDIFF(SECOND, CURDATE(), NOW())");
                            DBUtil.asyncResultSetNext(rs2);
                            long secondsOfDay = rs2.getLong(1);
                            return secondsBehindMaster > secondsOfDay;
                        } else {
                            return true;
                        }
                    } else {
                        LOG.warn("SHOW SLAVE STATUS has no result");
                    }
                }
            } else {
                LOG.warn("SHOW VARIABLES like 'read_only' has no result");
            }
        } catch (Exception e) {
            LOG.warn("checkSlave failed, errorMessage:[{}].", e.getMessage());
        }
        return false;
    }

    /**
     * 检查表是否具有insert 权限
     * insert on *.* 或者 insert on database.* 时验证通过
     * 当insert on database.tableName时，确保tableList中的所有table有insert 权限，验证通过
     * 其它验证都不通过
     *
     * @author ZiChi
     * @version 1.0 2015-01-28
     */
    public static boolean hasInsertPrivilege(DataBaseType dataBaseType, String jdbcURL, String userName, String password, List<String> tableList) {
        /*准备参数*/

        String[] urls = jdbcURL.split("/");
        String dbName;
        if (urls != null && urls.length != 0) {
            dbName = urls[3];
        }else{
            return false;
        }

        String dbPattern = "`" + dbName + "`.*";
        Collection<String> tableNames = new HashSet<String>(tableList.size());
        tableNames.addAll(tableList);

        Connection connection = connect(dataBaseType, jdbcURL, userName, password);
        try {
            ResultSet rs = query(connection, "SHOW GRANTS FOR " + userName);
            while (DBUtil.asyncResultSetNext(rs)) {
                String grantRecord = rs.getString("Grants for " + userName + "@%");
                String[] params = grantRecord.split("\\`");
                if (params != null && params.length >= 3) {
                    String tableName = params[3];
                    if (params[0].contains("INSERT") && !tableName.equals("*") && tableNames.contains(tableName))
                        tableNames.remove(tableName);
                } else {
                    if (grantRecord.contains("INSERT") ||grantRecord.contains("ALL PRIVILEGES")) {
                        if (grantRecord.contains("*.*"))
                            return true;
                        else if (grantRecord.contains(dbPattern)) {
                            return true;
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Check the database has the Insert Privilege failed, errorMessage:[{}]", e.getMessage());
        }
        if (tableNames.isEmpty())
            return true;
        return false;
    }


    public static boolean checkInsertPrivilege(DataBaseType dataBaseType, String jdbcURL, String userName, String password, List<String> tableList) {
        Connection connection = connect(dataBaseType, jdbcURL, userName, password);
        String insertTemplate = "insert into %s(select * from %s where 1 = 2)";

        boolean hasInsertPrivilege = true;
        Statement insertStmt = null;
        for(String tableName : tableList) {
            String checkInsertPrivilegeSql = String.format(insertTemplate, tableName, tableName);
            try {
                insertStmt = connection.createStatement();
                executeSqlWithoutResultSet(insertStmt, checkInsertPrivilegeSql);
            } catch (Exception e) {
                if(DataBaseType.Oracle.equals(dataBaseType)) {
                    if(e.getMessage() != null && e.getMessage().contains("insufficient privileges")) {
                        hasInsertPrivilege = false;
                        LOG.warn("User [" + userName +"] has no 'insert' privilege on table[" + tableName + "], errorMessage:[{}]", e.getMessage());
                    }
                } else {
                    hasInsertPrivilege = false;
                    LOG.warn("User [" + userName + "] has no 'insert' privilege on table[" + tableName + "], errorMessage:[{}]", e.getMessage());
                }
            }
        }
        try {
            connection.close();
        } catch (SQLException e) {
            LOG.warn("connection close failed, " + e.getMessage());
        }
        return hasInsertPrivilege;
    }

    public static boolean checkDeletePrivilege(DataBaseType dataBaseType,String jdbcURL, String userName, String password, List<String> tableList) {
        Connection connection = connect(dataBaseType, jdbcURL, userName, password);
        String deleteTemplate = "delete from %s WHERE 1 = 2";

        boolean hasInsertPrivilege = true;
        Statement deleteStmt = null;
        for(String tableName : tableList) {
            String checkDeletePrivilegeSQL = String.format(deleteTemplate, tableName);
            try {
                deleteStmt = connection.createStatement();
                executeSqlWithoutResultSet(deleteStmt, checkDeletePrivilegeSQL);
            } catch (Exception e) {
                hasInsertPrivilege = false;
                LOG.warn("User [" + userName +"] has no 'delete' privilege on table[" + tableName + "], errorMessage:[{}]", e.getMessage());
            }
        }
        try {
            connection.close();
        } catch (SQLException e) {
            LOG.warn("connection close failed, " + e.getMessage());
        }
        return hasInsertPrivilege;
    }

    public static boolean needCheckDeletePrivilege(Configuration originalConfig) {
        List<String> allSqls =new ArrayList<String>();
        List<String> preSQLs = originalConfig.getList(Key.PRE_SQL, String.class);
        List<String> postSQLs = originalConfig.getList(Key.POST_SQL, String.class);
        if (preSQLs != null && !preSQLs.isEmpty()){
            allSqls.addAll(preSQLs);
        }
        if (postSQLs != null && !postSQLs.isEmpty()){
            allSqls.addAll(postSQLs);
        }
        for(String sql : allSqls) {
            if(StringUtils.isNotBlank(sql)) {
                if (sql.trim().toUpperCase().startsWith("DELETE")) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Get direct JDBC connection
     * <p/>
     * if connecting failed, try to connect for MAX_TRY_TIMES times
     * <p/>
     * NOTE: In DataX, we don't need connection pool in fact
     */
    public static Connection getConnection(final DataBaseType dataBaseType,
                                           final String jdbcUrl, final String username, final String password) {

        return getConnection(dataBaseType, jdbcUrl, username, password, String.valueOf(Constant.SOCKET_TIMEOUT_INSECOND * 1000));
    }

    /**
     *
     * @param dataBaseType
     * @param jdbcUrl
     * @param username
     * @param password
     * @param socketTimeout 设置socketTimeout，单位ms，String类型
     * @return
     */
    public static Connection getConnection(final DataBaseType dataBaseType,
                                           final String jdbcUrl, final String username, final String password, final String socketTimeout) {

        try {
            return RetryUtil.executeWithRetry(new Callable<Connection>() {
                @Override
                public Connection call() throws Exception {
                    return DBUtil.connect(dataBaseType, jdbcUrl, username,
                            password, socketTimeout);
                }
            }, 9, 1000L, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.CONN_DB_ERROR,
                    String.format("数据库连接失败. 因为根据您配置的连接信息:%s获取数据库连接失败. 请检查您的配置并作出修改.", jdbcUrl), e);
        }
    }

    /**
     * Get direct JDBC connection
     * <p/>
     * if connecting failed, try to connect for MAX_TRY_TIMES times
     * <p/>
     * NOTE: In DataX, we don't need connection pool in fact
     */
    public static Connection getConnectionWithoutRetry(final DataBaseType dataBaseType,
                                                       final String jdbcUrl, final String username, final String password) {
        return getConnectionWithoutRetry(dataBaseType, jdbcUrl, username,
                password, String.valueOf(Constant.SOCKET_TIMEOUT_INSECOND * 1000));
    }

    public static Connection getConnectionWithoutRetry(final DataBaseType dataBaseType,
                                                       final String jdbcUrl, final String username, final String password, String socketTimeout) {
        return DBUtil.connect(dataBaseType, jdbcUrl, username,
                password, socketTimeout);
    }

    private static synchronized Connection connect(DataBaseType dataBaseType,
                                                   String url, String user, String pass) {
        return connect(dataBaseType, url, user, pass, String.valueOf(Constant.SOCKET_TIMEOUT_INSECOND * 1000));
    }

    private static synchronized Connection connect(DataBaseType dataBaseType,
                                                   String url, String user, String pass, String socketTimeout) {

        //ob10的处理
        if (url.startsWith(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING) && dataBaseType == DataBaseType.MySql) {
            String[] ss = url.split(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING_PATTERN);
            if (ss.length != 3) {
                throw DataXException
                        .asDataXException(
                                DBUtilErrorCode.JDBC_OB10_ADDRESS_ERROR, "JDBC OB10格式错误，请联系askdatax");
            }
            LOG.info("this is ob1_0 jdbc url.");
            user = ss[1].trim() +":"+user;
            url = ss[2];
            LOG.info("this is ob1_0 jdbc url. user="+user+" :url="+url);
        }

        Properties prop = new Properties();
        prop.put("user", user);
        prop.put("password", pass);

        if (dataBaseType == DataBaseType.Oracle) {
            //oracle.net.READ_TIMEOUT for jdbc versions < 10.1.0.5 oracle.jdbc.ReadTimeout for jdbc versions >=10.1.0.5
            // unit ms
            prop.put("oracle.jdbc.ReadTimeout", socketTimeout);
        }

        return connect(dataBaseType, url, prop);
    }

    private static synchronized Connection connect(DataBaseType dataBaseType,
                                                   String url, Properties prop) {
        try {
            Class.forName(dataBaseType.getDriverClassName());
            DriverManager.setLoginTimeout(Constant.TIMEOUT_SECONDS);
            return DriverManager.getConnection(url, prop);
        } catch (Exception e) {
            throw RdbmsException.asConnException(dataBaseType, e, prop.getProperty("user"), null);
        }
    }

    /**
     * a wrapped method to execute select-like sql statement .
     *
     * @param conn Database connection .
     * @param sql  sql statement to be executed
     * @return a {@link ResultSet}
     * @throws SQLException if occurs SQLException.
     */
    public static ResultSet query(Connection conn, String sql, int fetchSize)
            throws SQLException {
        // 默认3600 s 的query Timeout
        return query(conn, sql, fetchSize, Constant.SOCKET_TIMEOUT_INSECOND);
    }

    /**
     * a wrapped method to execute select-like sql statement .
     *
     * @param conn         Database connection .
     * @param sql          sql statement to be executed
     * @param fetchSize
     * @param queryTimeout unit:second
     * @return
     * @throws SQLException
     */
    public static ResultSet query(Connection conn, String sql, int fetchSize, int queryTimeout)
            throws SQLException {
        // make sure autocommit is off
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(fetchSize);
        stmt.setQueryTimeout(queryTimeout);
        return query(stmt, sql);
    }

    /**
     * a wrapped method to execute select-like sql statement .
     *
     * @param stmt {@link Statement}
     * @param sql  sql statement to be executed
     * @return a {@link ResultSet}
     * @throws SQLException if occurs SQLException.
     */
    public static ResultSet query(Statement stmt, String sql)
            throws SQLException {
        return stmt.executeQuery(sql);
    }

    public static void executeSqlWithoutResultSet(Statement stmt, String sql)
            throws SQLException {
        stmt.execute(sql);
    }

    /**
     * Close {@link ResultSet}, {@link Statement} referenced by this
     * {@link ResultSet}
     *
     * @param rs {@link ResultSet} to be closed
     * @throws IllegalArgumentException
     */
    public static void closeResultSet(ResultSet rs) {
        try {
            if (null != rs) {
                Statement stmt = rs.getStatement();
                if (null != stmt) {
                    stmt.close();
                    stmt = null;
                }
                rs.close();
            }
            rs = null;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void closeDBResources(ResultSet rs, Statement stmt,
                                        Connection conn) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException unused) {
            }
        }

        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException unused) {
            }
        }

        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException unused) {
            }
        }
    }

    public static void closeDBResources(Statement stmt, Connection conn) {
        closeDBResources(null, stmt, conn);
    }

    public static List<String> getTableColumns(DataBaseType dataBaseType,
                                               String jdbcUrl, String user, String pass, String tableName) {
        Connection conn = getConnection(dataBaseType, jdbcUrl, user, pass);
        return getTableColumnsByConn(dataBaseType, conn, tableName, "jdbcUrl:"+jdbcUrl);
    }

    public static List<String> getTableColumnsByConn(DataBaseType dataBaseType, Connection conn, String tableName, String basicMsg) {
        List<String> columns = new ArrayList<String>();
        Statement statement = null;
        ResultSet rs = null;
        String queryColumnSql = null;
        try {
            statement = conn.createStatement();
            queryColumnSql = String.format("select * from %s where 1=2",
                    tableName);
            rs = statement.executeQuery(queryColumnSql);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {
                columns.add(rsMetaData.getColumnName(i + 1));
            }

        } catch (SQLException e) {
            throw RdbmsException.asQueryException(dataBaseType,e,queryColumnSql,tableName,null);
        } finally {
            DBUtil.closeDBResources(rs, statement, conn);
        }

        return columns;
    }

    /**
     * @return Left:ColumnName Middle:ColumnType Right:ColumnTypeName
     */
    public static Triple<List<String>, List<Integer>, List<String>> getColumnMetaData(
            DataBaseType dataBaseType, String jdbcUrl, String user,
            String pass, String tableName, String column) {
        Connection conn = null;
        try {
            conn = getConnection(dataBaseType, jdbcUrl, user, pass);
            return getColumnMetaData(conn, tableName, column);
        } finally {
            DBUtil.closeDBResources(null, null, conn);
        }
    }

    /**
     * @return Left:ColumnName Middle:ColumnType Right:ColumnTypeName
     */
    public static Triple<List<String>, List<Integer>, List<String>> getColumnMetaData(
            Connection conn, String tableName, String column) {
        Statement statement = null;
        ResultSet rs = null;

        Triple<List<String>, List<Integer>, List<String>> columnMetaData = new ImmutableTriple<List<String>, List<Integer>, List<String>>(
                new ArrayList<String>(), new ArrayList<Integer>(),
                new ArrayList<String>());
        try {
            statement = conn.createStatement();
            String queryColumnSql = "select " + column + " from " + tableName
                    + " where 1=2";

            rs = statement.executeQuery(queryColumnSql);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {

                columnMetaData.getLeft().add(rsMetaData.getColumnName(i + 1));
                columnMetaData.getMiddle().add(rsMetaData.getColumnType(i + 1));
                columnMetaData.getRight().add(
                        rsMetaData.getColumnTypeName(i + 1));
            }
            return columnMetaData;

        } catch (SQLException e) {
            throw DataXException
                    .asDataXException(DBUtilErrorCode.GET_COLUMN_INFO_FAILED,
                            String.format("获取表:%s 的字段的元信息时失败. 请联系 DBA 核查该库、表信息.", tableName), e);
        } finally {
            DBUtil.closeDBResources(rs, statement, null);
        }
    }

    public static boolean testConnWithoutRetry(DataBaseType dataBaseType,
                                               String url, String user, String pass, boolean checkSlave){
        Connection connection = null;

        try {
            connection = connect(dataBaseType, url, user, pass);
            if (connection != null) {
                if (dataBaseType.equals(dataBaseType.MySql) && checkSlave) {
                    //dataBaseType.MySql
                    boolean connOk = !isSlaveBehind(connection);
                    return connOk;
                } else {
                    return true;
                }
            }
        } catch (Exception e) {
            LOG.warn("test connection of [{}] failed, for {}.", url,
                    e.getMessage());
        } finally {
            DBUtil.closeDBResources(null, connection);
        }
        return false;
    }

    public static boolean testConnWithoutRetry(DataBaseType dataBaseType,
                                               String url, String user, String pass, List<String> preSql) {
        Connection connection = null;
        try {
            connection = connect(dataBaseType, url, user, pass);
            if (null != connection) {
                for (String pre : preSql) {
                    if (doPreCheck(connection, pre) == false) {
                        LOG.warn("doPreCheck failed.");
                        return false;
                    }
                }
                return true;
            }
        } catch (Exception e) {
            LOG.warn("test connection of [{}] failed, for {}.", url,
                    e.getMessage());
        } finally {
            DBUtil.closeDBResources(null, connection);
        }

        return false;
    }

    public static boolean isOracleMaster(final String url, final String user, final String pass) {
        try {
            return RetryUtil.executeWithRetry(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    Connection conn = null;
                    try {
                        conn = connect(DataBaseType.Oracle, url, user, pass);
                        ResultSet rs = query(conn, "select DATABASE_ROLE from V$DATABASE");
                        if (DBUtil.asyncResultSetNext(rs, 5)) {
                            String role = rs.getString("DATABASE_ROLE");
                            return "PRIMARY".equalsIgnoreCase(role);
                        }
                        throw DataXException.asDataXException(DBUtilErrorCode.RS_ASYNC_ERROR,
                                String.format("select DATABASE_ROLE from V$DATABASE failed,请检查您的jdbcUrl:%s.", url));
                    } finally {
                        DBUtil.closeDBResources(null, conn);
                    }
                }
            }, 3, 1000L, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(DBUtilErrorCode.CONN_DB_ERROR,
                    String.format("select DATABASE_ROLE from V$DATABASE failed, url: %s", url), e);
        }
    }

    public static ResultSet query(Connection conn, String sql)
            throws SQLException {
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        //默认3600 seconds
        stmt.setQueryTimeout(Constant.SOCKET_TIMEOUT_INSECOND);
        return query(stmt, sql);
    }

    private static boolean doPreCheck(Connection conn, String pre) {
        ResultSet rs = null;
        try {
            rs = query(conn, pre);

            int checkResult = -1;
            if (DBUtil.asyncResultSetNext(rs)) {
                checkResult = rs.getInt(1);
                if (DBUtil.asyncResultSetNext(rs)) {
                    LOG.warn(
                            "pre check failed. It should return one result:0, pre:[{}].",
                            pre);
                    return false;
                }

            }

            if (0 == checkResult) {
                return true;
            }

            LOG.warn(
                    "pre check failed. It should return one result:0, pre:[{}].",
                    pre);
        } catch (Exception e) {
            LOG.warn("pre check failed. pre:[{}], errorMessage:[{}].", pre,
                    e.getMessage());
        } finally {
            DBUtil.closeResultSet(rs);
        }
        return false;
    }

    // warn:until now, only oracle need to handle session config.
    public static void dealWithSessionConfig(Connection conn,
                                             Configuration config, DataBaseType databaseType, String message) {
        List<String> sessionConfig = null;
        switch (databaseType) {
            case Oracle:
                sessionConfig = config.getList(Key.SESSION,
                        new ArrayList<String>(), String.class);
                DBUtil.doDealWithSessionConfig(conn, sessionConfig, message);
                break;
            case DRDS:
                // 用于关闭 drds 的分布式事务开关
                sessionConfig = new ArrayList<String>();
                sessionConfig.add("set transaction policy 4");
                DBUtil.doDealWithSessionConfig(conn, sessionConfig, message);
                break;
            case MySql:
                sessionConfig = config.getList(Key.SESSION,
                        new ArrayList<String>(), String.class);
                DBUtil.doDealWithSessionConfig(conn, sessionConfig, message);
                break;
            default:
                break;
        }
    }

    private static void doDealWithSessionConfig(Connection conn,
                                                List<String> sessions, String message) {
        if (null == sessions || sessions.isEmpty()) {
            return;
        }

        Statement stmt;
        try {
            stmt = conn.createStatement();
        } catch (SQLException e) {
            throw DataXException
                    .asDataXException(DBUtilErrorCode.SET_SESSION_ERROR, String
                                    .format("session配置有误. 因为根据您的配置执行 session 设置失败. 上下文信息是:[%s]. 请检查您的配置并作出修改.", message),
                            e);
        }

        for (String sessionSql : sessions) {
            LOG.info("execute sql:[{}]", sessionSql);
            try {
                DBUtil.executeSqlWithoutResultSet(stmt, sessionSql);
            } catch (SQLException e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.SET_SESSION_ERROR, String.format(
                                "session配置有误. 因为根据您的配置执行 session 设置失败. 上下文信息是:[%s]. 请检查您的配置并作出修改.", message), e);
            }
        }
        DBUtil.closeDBResources(stmt, null);
    }

    public static void sqlValid(String sql, DataBaseType dataBaseType){
        SQLStatementParser statementParser = SQLParserUtils.createSQLStatementParser(sql,dataBaseType.getTypeName());
        statementParser.parseStatementList();
    }

    /**
     * 异步获取resultSet的next(),注意，千万不能应用在数据的读取中。只能用在meta的获取
     * @param resultSet
     * @return
     */
    public static boolean asyncResultSetNext(final ResultSet resultSet) {
        return asyncResultSetNext(resultSet, 3600);
    }

    public static boolean asyncResultSetNext(final ResultSet resultSet, int timeout) {
        Future<Boolean> future = rsExecutors.get().submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return resultSet.next();
            }
        });
        try {
            return future.get(timeout, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.RS_ASYNC_ERROR, "异步获取ResultSet失败", e);
        }
    }
    
    public static void loadDriverClass(String pluginType, String pluginName) {
        try {
            String pluginJsonPath = StringUtils.join(
                    new String[] { System.getProperty("datax.home"), "plugin",
                            pluginType,
                            String.format("%s%s", pluginName, pluginType),
                            "plugin.json" }, File.separator);
            Configuration configuration = Configuration.from(new File(
                    pluginJsonPath));
            List<String> drivers = configuration.getList("drivers",
                    String.class);
            for (String driver : drivers) {
                Class.forName(driver);
            }
        } catch (ClassNotFoundException e) {
            throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                    "数据库驱动加载错误, 请确认libs目录有驱动jar包且plugin.json中drivers配置驱动类正确!",
                    e);
        }
    }
}
