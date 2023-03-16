package com.alibaba.datax.plugin.writer.hbase11xsqlwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yanghan.y
 */
public class HbaseSQLHelper {
    private static final Logger LOG = LoggerFactory.getLogger(HbaseSQLHelper.class);

    public static ThinClientPTable ptable;

    /**
     * 将datax的配置解析成sql writer的配置
     */
    public static HbaseSQLWriterConfig parseConfig(Configuration cfg) {
        return HbaseSQLWriterConfig.parse(cfg);
    }

    /**
     * 将hbase config字符串解析成zk quorum和znode。
     * 因为hbase使用的配置名称 xxx.xxxx.xxx会被{@link Configuration#from(String)}识别成json路径，
     * 而不是一个完整的配置项，所以，hbase的配置必须通过直接调用json API进行解析。
     * @param hbaseCfgString 配置中{@link Key#HBASE_CONFIG}的值
     * @return 返回2个string，第一个是zk quorum,第二个是znode
     */
    public static Pair<String, String> getHbaseConfig(String hbaseCfgString) {
        assert hbaseCfgString != null;
        Map<String, String> hbaseConfigMap = JSON.parseObject(hbaseCfgString, new TypeReference<Map<String, String>>() {});
        String zkQuorum = hbaseConfigMap.get(Key.HBASE_ZK_QUORUM);
        String znode = hbaseConfigMap.get(Key.HBASE_ZNODE_PARENT);
        if (znode == null || znode.isEmpty()) {
            znode = Constant.DEFAULT_ZNODE;
        }
        return new Pair<String, String>(zkQuorum, znode);
    }

    public static Map<String, String> getThinConnectConfig(String hbaseCfgString) {
        assert hbaseCfgString != null;
        return JSON.parseObject(hbaseCfgString, new TypeReference<Map<String, String>>() {});
    }

    /**
     * 校验配置
     */
    public static void validateConfig(HbaseSQLWriterConfig cfg) {
        // 校验集群地址：尝试连接，连不上就说明有问题，抛错退出
        Connection conn = getJdbcConnection(cfg);

        // 检查表:存在，可用
        checkTable(conn, cfg.getNamespace(), cfg.getTableName(), cfg.isThinClient());

        // 校验元数据：配置中给出的列必须是目的表中已经存在的列
        PTable schema = null;
        try {
            schema = getTableSchema(conn, cfg.getNamespace(), cfg.getTableName(), cfg.isThinClient());
        } catch (SQLException e) {
            throw DataXException.asDataXException(HbaseSQLWriterErrorCode.GET_HBASE_CONNECTION_ERROR,
                    "无法获取目的表" + cfg.getTableName() + "的元数据信息，表可能不是SQL表或表名配置错误，请检查您的配置 或者 联系 HBase 管理员.", e);
        }

        try {
            List<String> columnNames = cfg.getColumns();
            for (String colName : columnNames) {
                schema.getColumnForColumnName(colName);
            }
        } catch (ColumnNotFoundException e) {
            // 用户配置的列名在元数据中不存在
            throw DataXException.asDataXException(HbaseSQLWriterErrorCode.ILLEGAL_VALUE,
                    "您配置的列" + e.getColumnName() + "在目的表" + cfg.getTableName() + "的元数据中不存在，请检查您的配置 或者 联系 HBase 管理员.", e);
        } catch (SQLException e) {
            // 列名有二义性或者其他问题
            throw DataXException.asDataXException(HbaseSQLWriterErrorCode.ILLEGAL_VALUE,
                    "目的表" + cfg.getTableName() + "的列信息校验失败，请检查您的配置 或者 联系 HBase 管理员.", e);
        }
    }

    /**
     * 获取JDBC连接，轻量级连接，使用完后必须显式close
     */
    public static Connection getJdbcConnection(HbaseSQLWriterConfig cfg) {
        String connStr = cfg.getConnectionString();
        LOG.debug("Connecting to HBase cluster [" + connStr + "] ...");
        Connection conn;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            if (cfg.isThinClient()) {
                conn = getThinClientJdbcConnection(cfg);
            } else {
                conn = DriverManager.getConnection(connStr);
            }
            conn.setAutoCommit(false);
        } catch (Throwable e) {
            throw DataXException.asDataXException(HbaseSQLWriterErrorCode.GET_HBASE_CONNECTION_ERROR,
                    "无法连接hbase集群，配置不正确或目标集群不可用，请检查配置和集群状态 或者 联系 HBase 管理员.", e);
        }
        LOG.debug("Connected to HBase cluster successfully.");
        return conn;
    }

    /**
     * 创建 thin client jdbc连接
     * @param cfg
     * @return
     * @throws SQLException
     */
    public static Connection getThinClientJdbcConnection(HbaseSQLWriterConfig cfg) throws SQLException {
        String connStr = cfg.getConnectionString();
        LOG.info("Connecting to HBase cluster [" + connStr + "] use thin client ...");
        Connection conn = DriverManager.getConnection(connStr, cfg.getUsername(), cfg.getPassword());
        String userNamespaceQuery = "use " + cfg.getNamespace();
        Statement statement = null;
        try {
          statement = conn.createStatement();
          statement.executeUpdate(userNamespaceQuery);
          return conn;
        } catch (Exception e) {
          throw DataXException.asDataXException(HbaseSQLWriterErrorCode.GET_HBASE_CONNECTION_ERROR,
              "无法连接配置的namespace, 请检查配置 或者 联系 HBase 管理员.", e);
        } finally {
          if (statement != null) {
            statement.close();
          }
        }
    }

    /**
     * 获取一张表的元数据信息
     * @param conn hbsae sql的jdbc连接
     * @param fullTableName 目标表的完整表名
     * @return 表的元数据
     */
    public static PTable getTableSchema(Connection conn, String fullTableName) throws SQLException {
        PhoenixConnection hconn = conn.unwrap(PhoenixConnection.class);
        MetaDataClient mdc = new MetaDataClient(hconn);
        String schemaName = SchemaUtil.getSchemaNameFromFullName(fullTableName);
        String tableName = SchemaUtil.getTableNameFromFullName(fullTableName);
        return mdc.updateCache(schemaName, tableName).getTable();
    }

    /**
     *  获取一张表的元数据信息
     * @param conn
     * @param namespace
     * @param fullTableName
     * @param isThinClient  是否使用thin client
     * @return 表的元数据
     * @throws SQLException
     */
    public static PTable getTableSchema(Connection conn, String namespace, String fullTableName, boolean isThinClient)
        throws
        SQLException {
        LOG.info("Start to get table schema of namespace=" + namespace + " , fullTableName=" + fullTableName);
        if (!isThinClient) {
            return getTableSchema(conn, fullTableName);
        } else {
            if (ptable == null) {
                ResultSet result = conn.getMetaData().getColumns(null, namespace, fullTableName, null);
                try {
                    ThinClientPTable retTable = new ThinClientPTable();
                    retTable.setColTypeMap(parseColType(result));
                    ptable = retTable;
                }finally {
                    if (result != null) {
                        result.close();
                    }
                }
            }
            return ptable;
        }

    }

    /**
     * 解析字段
     * @param rs
     * @return
     * @throws SQLException
     */
    public static Map<String, ThinClientPTable.ThinClientPColumn>  parseColType(ResultSet rs) throws SQLException {
        Map<String, ThinClientPTable.ThinClientPColumn> cols = new HashMap<String, ThinClientPTable
            .ThinClientPColumn>();
        ResultSetMetaData md = rs.getMetaData();
        int columnCount = md.getColumnCount();

        while (rs.next()) {
            String colName  = null;
            PDataType colType = null;
            for (int i = 1; i <= columnCount; i++) {
                if (md.getColumnLabel(i).equals("TYPE_NAME")) {
                    colType = PDataType.fromSqlTypeName((String) rs.getObject(i));
                } else if (md.getColumnLabel(i).equals("COLUMN_NAME")) {
                    colName = (String) rs.getObject(i);
                }
            }
            if (colType == null || colName == null) {
                throw new SQLException("ColType or colName is null, colType : " + colType + " , colName : " + colName);
            }
            cols.put(colName, new ThinClientPTable.ThinClientPColumn(colName, colType));
        }
        return cols;
    }


    /**
     * 清空表
     */
    public static void truncateTable(Connection conn, String tableName) {
        PhoenixConnection sqlConn = null;
        Admin admin = null;
        try {
            sqlConn = conn.unwrap(PhoenixConnection.class);
            admin = sqlConn.getQueryServices().getAdmin();
            TableName hTableName = TableName.valueOf(tableName);
            // 确保表存在、可用
            checkTable(admin, hTableName);
            // 清空表
            admin.disableTable(hTableName);
            admin.truncateTable(hTableName, true);
            LOG.debug("Table " + tableName + " has been truncated.");
        } catch (Throwable t) {
            // 清空表失败
            throw DataXException.asDataXException(HbaseSQLWriterErrorCode.TRUNCATE_HBASE_ERROR,
                    "清空目的表" + tableName + "失败，请联系 HBase 管理员.", t);
        } finally {
            if (admin != null) {
                closeAdmin(admin);
            }
        }
    }

    /**
     * 检查表
     * @param conn
     * @param namespace
     * @param tableName
     * @param isThinClient
     * @throws DataXException
     */
    public static void checkTable(Connection conn, String namespace, String tableName, boolean isThinClient)
        throws DataXException {
        if (!isThinClient) {
            checkTable(conn, tableName);
        } else {
            //ignore check table when use thin client
        }
    }


    /**
     * 检查表：表要存在，enabled
     */
    public static void checkTable(Connection conn, String tableName) throws DataXException {
        PhoenixConnection sqlConn = null;
        Admin admin = null;
        try {
            sqlConn = conn.unwrap(PhoenixConnection.class);
            admin = sqlConn.getQueryServices().getAdmin();
            TableName hTableName = TableName.valueOf(tableName);
            checkTable(admin, hTableName);
        } catch (SQLException t) {
            throw DataXException.asDataXException(HbaseSQLWriterErrorCode.TRUNCATE_HBASE_ERROR,
                    "表" + tableName + "状态检查未通过，请检查您的集群和表状态 或者 联系 Hbase 管理员.", t);
        } catch (IOException t) {
            throw DataXException.asDataXException(HbaseSQLWriterErrorCode.TRUNCATE_HBASE_ERROR,
                    "表" + tableName + "状态检查未通过，请检查您的集群和表状态 或者 联系 Hbase 管理员.", t);
        } finally {
            if (admin != null) {
                closeAdmin(admin);
            }
        }
    }

    private static void checkTable(Admin admin, TableName tableName) throws IOException {
        if(!admin.tableExists(tableName)){
            throw DataXException.asDataXException(HbaseSQLWriterErrorCode.ILLEGAL_VALUE,
                    "HBase目的表" + tableName.toString() + "不存在, 请检查您的配置 或者 联系 Hbase 管理员.");
        }
        if(!admin.isTableAvailable(tableName)){
            throw DataXException.asDataXException(HbaseSQLWriterErrorCode.ILLEGAL_VALUE,
                    "HBase目的表" + tableName.toString() + "不可用, 请检查您的配置 或者 联系 Hbase 管理员.");
        }
        if(admin.isTableDisabled(tableName)){
            throw DataXException.asDataXException(HbaseSQLWriterErrorCode.ILLEGAL_VALUE,
                    "HBase目的表" + tableName.toString() + "不可用, 请检查您的配置 或者 联系 Hbase 管理员.");
        }
    }

    private static void closeAdmin(Admin admin){
        try {
            if(null != admin)
                admin.close();
        } catch (IOException e) {
            throw DataXException.asDataXException(HbaseSQLWriterErrorCode.CLOSE_HBASE_AMIN_ERROR, e);
        }
    }
}
