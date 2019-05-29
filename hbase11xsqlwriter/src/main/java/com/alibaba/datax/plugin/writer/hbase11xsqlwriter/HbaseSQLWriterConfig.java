package com.alibaba.datax.plugin.writer.hbase11xsqlwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * HBase SQL writer config
 *
 * @author yanghan.y
 */
public class HbaseSQLWriterConfig {
    private final static Logger LOG = LoggerFactory.getLogger(HbaseSQLWriterConfig.class);
    private Configuration originalConfig;   // 原始的配置数据

    // 集群配置
    private String connectionString;

    // 表配置
    private String tableName;
    private List<String> columns;           // 目的表的所有列的列名，包括主键和非主键，不包括时间列

    // 其他配置
    private NullModeType nullMode;
    private int batchSize;                  // 一次批量写入多少行
    private boolean truncate;               // 导入开始前是否要清空目的表
    private boolean isThinClient;
    private String namespace;
    private String username;
    private String password;

    /**
     * @return 获取原始的datax配置
     */
    public Configuration getOriginalConfig() {
        return originalConfig;
    }

    /**
     * @return 获取连接字符串，使用ZK模式
     */
    public String getConnectionString() {
        return connectionString;
    }

    /**
     * @return 获取表名
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @return 返回所有的列，包括主键列和非主键列，但不包括version列
     */
    public List<String> getColumns() {
        return columns;
    }

    /**
     *
     * @return
     */
    public NullModeType getNullMode() {
        return nullMode;
    }

    /**
     * @return 批量写入的最大行数
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * @return 在writer初始化的时候是否要清空目标表
     */
    public boolean truncate() {
        return truncate;
    }

    public boolean isThinClient() {
        return isThinClient;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getPassword() {
        return password;
    }

    public String getUsername() {
        return username;
    }

    /**
     * @param dataxCfg
     * @return
     */
    public static HbaseSQLWriterConfig parse(Configuration dataxCfg) {
        assert dataxCfg != null;
        HbaseSQLWriterConfig cfg = new HbaseSQLWriterConfig();
        cfg.originalConfig = dataxCfg;

        // 1. 解析集群配置
        parseClusterConfig(cfg, dataxCfg);

        // 2. 解析列配置
        parseTableConfig(cfg, dataxCfg);

        // 3. 解析其他配置
        cfg.nullMode = NullModeType.getByTypeName(dataxCfg.getString(Key.NULL_MODE, Constant.DEFAULT_NULL_MODE));
        cfg.batchSize = dataxCfg.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_ROW_COUNT);
        cfg.truncate = dataxCfg.getBool(Key.TRUNCATE, Constant.DEFAULT_TRUNCATE);
        cfg.isThinClient = dataxCfg.getBool(Key.THIN_CLIENT, Constant.DEFAULT_USE_THIN_CLIENT);

        // 4. 打印解析出来的配置
        LOG.info("HBase SQL writer config parsed:" + cfg.toString());

        return cfg;
    }

    private static void parseClusterConfig(HbaseSQLWriterConfig cfg, Configuration dataxCfg) {
        // 获取hbase集群的连接信息字符串
        String hbaseCfg = dataxCfg.getString(Key.HBASE_CONFIG);
        if (StringUtils.isBlank(hbaseCfg)) {
            // 集群配置必须存在且不为空
            throw DataXException.asDataXException(
                    HbaseSQLWriterErrorCode.REQUIRED_VALUE,
                    "读 Hbase 时需要配置hbaseConfig，其内容为 Hbase 连接信息，请联系 Hbase PE 获取该信息.");
        }


        if (dataxCfg.getBool(Key.THIN_CLIENT, Constant.DEFAULT_USE_THIN_CLIENT)) {
            Map<String, String> thinConnectConfig = HbaseSQLHelper.getThinConnectConfig(hbaseCfg);
            String thinConnectStr = thinConnectConfig.get(Key.HBASE_THIN_CONNECT_URL);
            cfg.namespace = thinConnectConfig.get(Key.HBASE_THIN_CONNECT_NAMESPACE);
            cfg.username = thinConnectConfig.get(Key.HBASE_THIN_CONNECT_USERNAME);
            cfg.password = thinConnectConfig.get(Key.HBASE_THIN_CONNECT_PASSWORD);
            if (Strings.isNullOrEmpty(thinConnectStr)) {
                throw DataXException.asDataXException(
                    HbaseSQLWriterErrorCode.ILLEGAL_VALUE,
                    "thinClient=true的轻客户端模式下HBase的hbase.thin.connect.url配置不能为空，请联系HBase PE获取该信息.");
            }
            if (Strings.isNullOrEmpty(cfg.namespace) || Strings.isNullOrEmpty(cfg.username) || Strings
                .isNullOrEmpty(cfg.password)) {
                throw DataXException.asDataXException(HbaseSQLWriterErrorCode.ILLEGAL_VALUE,
                    "thinClient=true的轻客户端模式下HBase的hbase.thin.connect.namespce|username|password配置不能为空，请联系HBase "
                        + "PE获取该信息.");
            }
            cfg.connectionString = thinConnectStr;
        } else {
            // 解析zk服务器和znode信息
            Pair<String, String> zkCfg;
            try {
                zkCfg = HbaseSQLHelper.getHbaseConfig(hbaseCfg);
            } catch (Throwable t) {
                // 解析hbase配置错误
                throw DataXException.asDataXException(
                    HbaseSQLWriterErrorCode.REQUIRED_VALUE,
                    "解析hbaseConfig出错，请确认您配置的hbaseConfig为合法的json数据格式，内容正确.");
            }
            String zkQuorum = zkCfg.getFirst();
            String znode = zkCfg.getSecond();
            if (zkQuorum == null || zkQuorum.isEmpty()) {
                throw DataXException.asDataXException(
                    HbaseSQLWriterErrorCode.ILLEGAL_VALUE,
                    "HBase的hbase.zookeeper.quorum配置不能为空，请联系HBase PE获取该信息.");
            }
            if (znode == null || znode.isEmpty()) {
                throw DataXException.asDataXException(
                    HbaseSQLWriterErrorCode.ILLEGAL_VALUE,
                    "HBase的zookeeper.znode.parent配置不能为空，请联系HBase PE获取该信息.");
            }

            // 生成sql使用的连接字符串， 格式： jdbc:phoenix:zk_quorum:2181:/znode_parent
            cfg.connectionString = "jdbc:phoenix:" + zkQuorum + ":2181:" + znode;
        }
    }

    private static void parseTableConfig(HbaseSQLWriterConfig cfg, Configuration dataxCfg) {
        // 解析并检查表名
        cfg.tableName = dataxCfg.getString(Key.TABLE);
        if (cfg.tableName == null || cfg.tableName.isEmpty()) {
            throw DataXException.asDataXException(
                    HbaseSQLWriterErrorCode.ILLEGAL_VALUE, "HBase的tableName配置不能为空,请检查并修改配置.");
        }
        try {
            TableName tn = TableName.valueOf(cfg.tableName);
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    HbaseSQLWriterErrorCode.ILLEGAL_VALUE,
                    "您配置的tableName(" + cfg.tableName + ")含有非法字符，请检查您的配置 或者 联系 Hbase 管理员.");
        }

        // 解析列配置
        cfg.columns = dataxCfg.getList(Key.COLUMN, String.class);
        if (cfg.columns == null || cfg.columns.isEmpty()) {
            throw DataXException.asDataXException(
                    HbaseSQLWriterErrorCode.ILLEGAL_VALUE, "HBase的columns配置不能为空,请添加目标表的列名配置.");
        }
    }

    @Override
    public String toString() {
        StringBuilder ret = new StringBuilder();
        // 集群配置
        ret.append("\n[jdbc]");
        ret.append(connectionString);
        ret.append("\n");

        // 表配置
        ret.append("[tableName]");
        ret.append(tableName);
        ret.append("\n");
        ret.append("[column]");
        for (String col : columns) {
            ret.append(col);
            ret.append(",");
        }
        ret.setLength(ret.length() - 1);
        ret.append("\n");

        // 其他配置
        ret.append("[nullMode]");
        ret.append(nullMode);
        ret.append("\n");
        ret.append("[batchSize]");
        ret.append(batchSize);
        ret.append("\n");
        ret.append("[truncate]");
        ret.append(truncate);
        ret.append("\n");

        return ret.toString();
    }

    /**
     * 禁止直接实例化本类，必须调用{@link #parse}接口来初始化
     */
    private HbaseSQLWriterConfig() {
    }
}
