package com.alibaba.datax.plugin.reader.obhbasereader;

import static com.alibaba.datax.plugin.reader.obhbasereader.Constant.DEFAULT_OB_TABLE_CLIENT_LOG_LEVEL;
import static com.alibaba.datax.plugin.reader.obhbasereader.Constant.DEFAULT_OB_TABLE_HBASE_LOG_LEVEL;
import static com.alibaba.datax.plugin.reader.obhbasereader.Constant.DEFAULT_USE_ODPMODE;
import static com.alibaba.datax.plugin.reader.obhbasereader.Constant.OB_COM_ALIPAY_TABLE_CLIENT_LOG_LEVEL;
import static com.alibaba.datax.plugin.reader.obhbasereader.Constant.OB_COM_ALIPAY_TABLE_HBASE_LOG_LEVEL;
import static com.alibaba.datax.plugin.reader.obhbasereader.Constant.OB_HBASE_LOG_PATH;
import static com.alibaba.datax.plugin.reader.obhbasereader.Constant.OB_TABLE_CLIENT_LOG_LEVEL;
import static com.alibaba.datax.plugin.reader.obhbasereader.Constant.OB_TABLE_CLIENT_PROPERTY;
import static com.alibaba.datax.plugin.reader.obhbasereader.Constant.OB_TABLE_HBASE_LOG_LEVEL;
import static com.alibaba.datax.plugin.reader.obhbasereader.Constant.OB_TABLE_HBASE_PROPERTY;
import static org.apache.commons.lang3.StringUtils.EMPTY;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.util.ObVersion;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.TableExpandUtil;
import com.alibaba.datax.plugin.reader.obhbasereader.enums.ModeType;
import com.alibaba.datax.plugin.reader.obhbasereader.ext.ServerConnectInfo;
import com.alibaba.datax.plugin.reader.obhbasereader.task.AbstractHbaseTask;
import com.alibaba.datax.plugin.reader.obhbasereader.task.SQLNormalModeReader;
import com.alibaba.datax.plugin.reader.obhbasereader.task.ScanMultiVersionReader;
import com.alibaba.datax.plugin.reader.obhbasereader.task.ScanNormalModeReader;
import com.alibaba.datax.plugin.reader.obhbasereader.util.HbaseSplitUtil;
import com.alibaba.datax.plugin.reader.obhbasereader.util.ObHbaseReaderUtil;
import com.alibaba.datax.plugin.reader.obhbasereader.util.SqlReaderSplitUtil;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.ObReaderUtils;

import com.google.common.base.Preconditions;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * ObHbaseReader 支持分库分表
 * 仅支持ob3.x及以上版本
 */
public class ObHbaseReader extends Reader {

    public static class Job extends Reader.Job {
        static private final String ACCESS_DENIED_ERROR = "Access denied for user";
        private static Logger LOG = LoggerFactory.getLogger(ObHbaseReader.class);
        private Configuration originalConfig;

        @Override
        public void init() {
            if (System.getProperty(OB_TABLE_CLIENT_PROPERTY) == null) {
                LOG.info(OB_TABLE_CLIENT_PROPERTY + " not set");
                System.setProperty(OB_TABLE_CLIENT_PROPERTY, OB_HBASE_LOG_PATH);
            }
            if (System.getProperty(OB_TABLE_HBASE_PROPERTY) == null) {
                LOG.info(OB_TABLE_HBASE_PROPERTY + " not set");
                System.setProperty(OB_TABLE_HBASE_PROPERTY, OB_HBASE_LOG_PATH);
            }
            if (System.getProperty(OB_TABLE_CLIENT_LOG_LEVEL) == null) {
                LOG.info(OB_TABLE_CLIENT_LOG_LEVEL + " not set");
                System.setProperty(OB_TABLE_CLIENT_LOG_LEVEL, DEFAULT_OB_TABLE_CLIENT_LOG_LEVEL);
            }
            if (System.getProperty(OB_TABLE_HBASE_LOG_LEVEL) == null) {
                LOG.info(OB_TABLE_HBASE_LOG_LEVEL + " not set");
                System.setProperty(OB_TABLE_HBASE_LOG_LEVEL, DEFAULT_OB_TABLE_HBASE_LOG_LEVEL);
            }
            if (System.getProperty(OB_COM_ALIPAY_TABLE_CLIENT_LOG_LEVEL) == null) {
                LOG.info(OB_COM_ALIPAY_TABLE_CLIENT_LOG_LEVEL + " not set");
                System.setProperty(OB_COM_ALIPAY_TABLE_CLIENT_LOG_LEVEL, DEFAULT_OB_TABLE_CLIENT_LOG_LEVEL);
            }
            if (System.getProperty(OB_COM_ALIPAY_TABLE_HBASE_LOG_LEVEL) == null) {
                LOG.info(OB_COM_ALIPAY_TABLE_HBASE_LOG_LEVEL + " not set");
                System.setProperty(OB_COM_ALIPAY_TABLE_HBASE_LOG_LEVEL, DEFAULT_OB_TABLE_HBASE_LOG_LEVEL);
            }

            LOG.info("{} is set to {}, {} is set to {}",
                    OB_TABLE_CLIENT_PROPERTY, OB_HBASE_LOG_PATH, OB_TABLE_HBASE_PROPERTY, OB_HBASE_LOG_PATH);
            this.originalConfig = super.getPluginJobConf();
            ObHbaseReaderUtil.doPretreatment(originalConfig);
            List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
            // 逻辑表配置
            Preconditions.checkArgument(CollectionUtils.isNotEmpty(conns), "connection information is empty.");
            dealLogicConnAndTable(conns);
            if (LOG.isDebugEnabled()) {
                LOG.debug("After init(), now originalConfig is:\n{}\n", this.originalConfig);
            }
        }

        @Override
        public void destroy() {
        }

        private void dealLogicConnAndTable(List<Object> conns) {
            String unifiedUsername = originalConfig.getString(Key.USERNAME);
            String unifiedPassword = originalConfig.getString(Key.PASSWORD);
            boolean useSqlReader = originalConfig.getBool(Key.USE_SQL_READER, com.alibaba.datax.plugin.reader.obhbasereader.Constant.DEFAULT_USE_SQLREADER);
            boolean checkSlave = originalConfig.getBool(com.alibaba.datax.plugin.rdbms.reader.Key.CHECK_SLAVE, false);
            Set<String> keywords = Arrays.stream(com.alibaba.datax.plugin.reader.obhbasereader.Constant.OBMYSQL_KEYWORDS.split(",")).collect(Collectors.toSet());
            List<String> preSql = originalConfig.getList(com.alibaba.datax.plugin.rdbms.reader.Key.PRE_SQL, String.class);

            int tableNum = 0;

            for (int i = 0, len = conns.size(); i < len; i++) {
                Configuration connConf = Configuration.from(conns.get(i).toString());
                String curUsername = connConf.getString(Key.USERNAME, unifiedUsername);
                Preconditions.checkArgument(StringUtils.isNotEmpty(curUsername), "username is empty.");
                String curPassword = connConf.getString(Key.PASSWORD, unifiedPassword);

                originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, i, Key.USERNAME), curUsername);
                originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, i, Key.PASSWORD), curPassword);

                List<String> jdbcUrls = connConf.getList(Key.JDBC_URL, new ArrayList<>(), String.class);
                String jdbcUrl;
                if (useSqlReader) {
                    // sql模式下，jdbcUrl必须配置，只有使用sql模式的情况才检查地址
                    Preconditions.checkArgument(CollectionUtils.isNotEmpty(jdbcUrls), "if using sql mode, jdbcUrl is needed");
                    jdbcUrl = DBUtil.chooseJdbcUrlWithoutRetry(DataBaseType.MySql, jdbcUrls, curUsername, curPassword, preSql, checkSlave);
                    jdbcUrl = DataBaseType.MySql.appendJDBCSuffixForReader(jdbcUrl);
                    // 回写到connection[i].jdbcUrl
                    originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, i, Key.JDBC_URL), jdbcUrl);
                    LOG.info("Available jdbcUrl:{}.", jdbcUrl);
                } else {
                    jdbcUrl = jdbcUrls.get(0);
                    jdbcUrl = StringUtils.isNotBlank(jdbcUrl) ? DataBaseType.MySql.appendJDBCSuffixForReader(jdbcUrl) : EMPTY;
                    checkAndSetHbaseConnConf(jdbcUrl, curUsername, curPassword, connConf, i);
                }

                // table 方式
                // 对每一个connection 上配置的table 项进行解析(已对表名称进行了 ` 处理的)
                List<String> tables = connConf.getList(Key.TABLE, String.class);

                List<String> expandedTables = TableExpandUtil.expandTableConf(DataBaseType.MySql, tables);

                if (expandedTables.isEmpty()) {
                    throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE, "The specified table list is empty.");
                }

                for (int ti = 0; ti < expandedTables.size(); ti++) {
                    String tableName = expandedTables.get(ti);
                    if (keywords.contains(tableName.toUpperCase())) {
                        expandedTables.set(ti, "`" + tableName + "`");
                    }
                }
                tableNum += expandedTables.size();
                originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, i, Key.TABLE), expandedTables);
            }

            if (tableNum == 0) {
                // 分库分表读，未匹配到可以抽取的表
                LOG.error("sharding rule result is empty.");
                throw DataXException.asDataXException("No tables were matched");
            }
            originalConfig.set(Constant.TABLE_NUMBER_MARK, tableNum);
        }

        /**
         * In public cloud, only odp mode can be used.
         * In private cloud, both odp mode and ocp mode can be used.
         *
         * @param jdbcUrl
         * @param curUsername
         * @param curPassword
         * @param connConf
         */
        private void checkAndSetHbaseConnConf(String jdbcUrl, String curUsername, String curPassword, Configuration connConf, int curIndex) {
            ServerConnectInfo serverConnectInfo = new ServerConnectInfo(jdbcUrl, curUsername, curPassword);
            if (!originalConfig.getBool(Key.USE_ODP_MODE, false)) {
                // Normally, only need to query at first time
                // In ocp mode, dbName, configUrl, sysUser and sysPass are needed.
                String sysUser = connConf.getString(Key.OB_SYS_USERNAME, originalConfig.getString(Key.OB_SYS_USERNAME));
                String sysPass = connConf.getString(Key.OB_SYS_PASSWORD, originalConfig.getString(Key.OB_SYS_PASSWORD));
                serverConnectInfo.setSysUser(sysUser);
                serverConnectInfo.setSysPass(sysPass);
                String configUrl = connConf.getString(Key.CONFIG_URL, originalConfig.getString(Key.CONFIG_URL));
                if (StringUtils.isBlank(configUrl)) {
                    configUrl = queryRsUrl(serverConnectInfo);
                }
                originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, curIndex, Key.USERNAME), curUsername);
                originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, curIndex, Key.OB_SYS_USERNAME), serverConnectInfo.sysUser);
                originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, curIndex, Key.OB_SYS_PASSWORD), serverConnectInfo.sysPass);
                originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, curIndex, Key.CONFIG_URL), configUrl);
            } else {
                // In odp mode, dbName, odp host and odp port are needed.
                String odpHost = connConf.getString(Key.ODP_HOST, serverConnectInfo.host);
                String odpPort = connConf.getString(Key.ODP_PORT, serverConnectInfo.port);
                originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, curIndex, Key.ODP_HOST), odpHost);
                originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, curIndex, Key.ODP_PORT), odpPort);
            }
            originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, curIndex, Key.DB_NAME), serverConnectInfo.databaseName);
        }

        private String queryRsUrl(ServerConnectInfo serverInfo) {
            Preconditions.checkArgument(checkVersionAfterV3(serverInfo.jdbcUrl, serverInfo.getFullUserName(), serverInfo.password), "ob before 3.x is not supported.");
            String configUrl = originalConfig.getString(Key.CONFIG_URL, null);
            if (configUrl == null) {
                try {
                    Connection conn = null;
                    int retry = 0;
                    final String sysJDBCUrl = serverInfo.jdbcUrl.replace(serverInfo.databaseName, "oceanbase");
                    do {
                        try {
                            if (retry > 0) {
                                int sleep = retry > 9 ? 500 : 1 << retry;
                                try {
                                    TimeUnit.SECONDS.sleep(sleep);
                                } catch (InterruptedException e) {
                                }
                                LOG.warn("retry fetch RsUrl the {} times", retry);
                            }
                            conn = DBUtil.getConnection(DataBaseType.OceanBase, sysJDBCUrl, serverInfo.sysUser, serverInfo.sysPass);
                            String sql = "show parameters like 'obconfig_url'";
                            LOG.info("query param: {}", sql);
                            PreparedStatement stmt = conn.prepareStatement(sql);
                            ResultSet result = stmt.executeQuery();
                            if (result.next()) {
                                configUrl = result.getString("Value");
                            }
                            if (StringUtils.isNotBlank(configUrl)) {
                                break;
                            }
                        } catch (Exception e) {
                            ++retry;
                            LOG.warn("fetch root server list(rsList) error {}", e.getMessage());
                        } finally {
                            DBUtil.closeDBResources(null, conn);
                        }
                    } while (retry < 3);

                    LOG.info("configure url is: " + configUrl);
                    originalConfig.set(Key.CONFIG_URL, configUrl);
                } catch (Exception e) {
                    LOG.error("Fail to get configure url: {}", e.getMessage(), e);
                    throw DataXException.asDataXException(HbaseReaderErrorCode.REQUIRED_VALUE, "未配置obConfigUrl，且无法获取obConfigUrl");
                }
            }
            return configUrl;
        }

        @Override
        public void prepare() {
        }

        @Override
        public void post() {
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            Map<String, HbaseColumnCell> hbaseColumnCells = ObHbaseReaderUtil.parseColumn(originalConfig.getList(Key.COLUMN, Map.class));
            if (hbaseColumnCells.size() == 0) {
                LOG.error("no column cells specified.");
                throw new RuntimeException("no column cells specified");
            }
            String columnFamily = ObHbaseReaderUtil.parseColumnFamily(hbaseColumnCells.values());
            Preconditions.checkArgument(StringUtils.isNotEmpty(columnFamily), "column family is empty.");
            List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
            Preconditions.checkArgument(conns != null && !conns.isEmpty(), "connection information is necessary.");
            return splitLogicTables(adviceNumber, conns, columnFamily);
        }

        private List<Configuration> splitLogicTables(int adviceNumber, List<Object> conns, String columnFamily) {
            // adviceNumber这里是channel数量大小, 即datax并发task数量
            // eachTableShouldSplittedNumber是单表应该切分的份数
            int eachTableShouldSplittedNumber = (int) Math.ceil(1.0 * adviceNumber / originalConfig.getInt(Constant.TABLE_NUMBER_MARK));
            boolean useSqlReader = originalConfig.getBool(Key.USE_SQL_READER, com.alibaba.datax.plugin.reader.obhbasereader.Constant.DEFAULT_USE_SQLREADER);
            boolean odpMode = originalConfig.getBool(Key.USE_ODP_MODE, DEFAULT_USE_ODPMODE);
            boolean readByPartition = originalConfig.getBool(Key.READ_BY_PARTITION, false);
            List<Configuration> splittedConfigs = new ArrayList<>();

            for (int i = 0, len = conns.size(); i < len; i++) {
                Configuration sliceConfig = originalConfig.clone();
                Configuration connConf = Configuration.from(conns.get(i).toString());
                copyConnConfByMode(useSqlReader, odpMode, sliceConfig, connConf);
                // 说明是配置的 table 方式
                // 已在之前进行了扩展和`处理，可以直接使用
                List<String> tables = connConf.getList(Key.TABLE, String.class);
                Validate.isTrue(null != tables && !tables.isEmpty(), "error in your configuration for the reading database table.");
                int tempEachTableShouldSplittedNumber = eachTableShouldSplittedNumber;
                if (tables.size() == 1) {
                    Integer splitFactor = originalConfig.getInt(com.alibaba.datax.plugin.rdbms.reader.Key.SPLIT_FACTOR, Constant.SPLIT_FACTOR);
                    tempEachTableShouldSplittedNumber = eachTableShouldSplittedNumber * splitFactor;
                }
                for (String table : tables) {
                    Configuration tempSlice;
                    tempSlice = sliceConfig.clone();
                    tempSlice.set(Key.TABLE, table);
                    splittedConfigs.addAll(
                            useSqlReader ? SqlReaderSplitUtil.splitSingleTable(tempSlice, table, columnFamily, tempEachTableShouldSplittedNumber, readByPartition) : HbaseSplitUtil.split(tempSlice));
                }
            }
            return splittedConfigs;
        }

        private void copyConnConfByMode(boolean useSqlReader, boolean odpMode, Configuration targetConf, Configuration sourceConnConf) {
            String username = sourceConnConf.getNecessaryValue(Key.USERNAME, DBUtilErrorCode.REQUIRED_VALUE);
            targetConf.set(Key.USERNAME, username);
            String password = sourceConnConf.getNecessaryValue(Key.PASSWORD, DBUtilErrorCode.REQUIRED_VALUE);
            targetConf.set(Key.PASSWORD, password);

            if (useSqlReader) {
                String jdbcUrl = sourceConnConf.getNecessaryValue(Key.JDBC_URL, DBUtilErrorCode.REQUIRED_VALUE);
                targetConf.set(Key.JDBC_URL, jdbcUrl);
            } else if (odpMode) {
                String dbName = sourceConnConf.getNecessaryValue(Key.DB_NAME, DBUtilErrorCode.REQUIRED_VALUE);
                targetConf.set(Key.DB_NAME, dbName);
                String odpHost = sourceConnConf.getNecessaryValue(Key.ODP_HOST, DBUtilErrorCode.REQUIRED_VALUE);
                targetConf.set(Key.ODP_HOST, odpHost);
                String odpPort = sourceConnConf.getNecessaryValue(Key.ODP_PORT, DBUtilErrorCode.REQUIRED_VALUE);
                targetConf.set(Key.ODP_PORT, odpPort);
            } else {
                String dbName = sourceConnConf.getNecessaryValue(Key.DB_NAME, DBUtilErrorCode.REQUIRED_VALUE);
                targetConf.set(Key.DB_NAME, dbName);
                String sysUser = sourceConnConf.getNecessaryValue(Key.OB_SYS_USERNAME, DBUtilErrorCode.REQUIRED_VALUE);
                targetConf.set(Key.OB_SYS_USERNAME, sysUser);
                String sysPass = sourceConnConf.getString(Key.OB_SYS_PASSWORD);
                targetConf.set(Key.OB_SYS_PASSWORD, sysPass);
            }
            targetConf.remove(Constant.CONN_MARK);
        }

        private boolean checkVersionAfterV3(String jdbcUrl, String username, String password) {
            int retryLimit = 3;
            int retryCount = 0;
            Connection conn = null;
            while (retryCount++ <= retryLimit) {
                try {
                    conn = DBUtil.getConnectionWithoutRetry(DataBaseType.MySql, jdbcUrl, username, password);
                    ObVersion obVersion = ObReaderUtils.getObVersion(conn);
                    return ObVersion.V3.compareTo(obVersion) <= 0;
                } catch (Exception e) {
                    LOG.error("fail to check ob version, will retry: " + e.getMessage());
                    if (e.getMessage().contains(ACCESS_DENIED_ERROR)) {
                        throw new RuntimeException(e);
                    }
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (Exception ex) {
                        LOG.error("interrupted while waiting for retry.");
                    }
                } finally {
                    DBUtil.closeDBResources(null, conn);
                }
            }
            return false;
        }
    }

    public static class Task extends Reader.Task {
        private static Logger LOG = LoggerFactory.getLogger(Task.class);
        private Configuration taskConfig;
        private AbstractHbaseTask hbaseTaskProxy;

        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();

            String mode = this.taskConfig.getString(Key.MODE);
            ModeType modeType = ModeType.getByTypeName(mode);
            boolean useSqlReader = this.taskConfig.getBool(Key.USE_SQL_READER, com.alibaba.datax.plugin.reader.obhbasereader.Constant.DEFAULT_USE_SQLREADER);
            LOG.info("init reader with mode: " + modeType);

            switch (modeType) {
                case Normal:
                    this.hbaseTaskProxy = useSqlReader ? new SQLNormalModeReader(this.taskConfig) : new ScanNormalModeReader(this.taskConfig);
                    break;
                case MultiVersionFixedColumn:
                    this.hbaseTaskProxy = new ScanMultiVersionReader(this.taskConfig);
                    break;
                default:
                    throw DataXException.asDataXException(HbaseReaderErrorCode.ILLEGAL_VALUE, "This type of mode is not supported by hbasereader:" + modeType);
            }
        }

        @Override
        public void destroy() {
            if (this.hbaseTaskProxy != null) {
                try {
                    this.hbaseTaskProxy.close();
                } catch (Exception e) {
                    //
                }
            }
        }

        @Override
        public void prepare() {
            try {
                this.hbaseTaskProxy.prepare();
            } catch (Exception e) {
                throw DataXException.asDataXException(HbaseReaderErrorCode.PREPAR_READ_ERROR, e);
            }
        }

        @Override
        public void post() {
            super.post();
        }

        @Override
        public void startRead(RecordSender recordSender) {
            Record record = recordSender.createRecord();
            boolean fetchOK;
            int retryTimes = 0;
            int maxRetryTimes = 3;
            while (true) {
                try {
                    // TODO check exception
                    fetchOK = this.hbaseTaskProxy.fetchLine(record);
                } catch (Exception e) {
                    LOG.info("fetch record failed. reason: {}.", e.getMessage(), e);
                    super.getTaskPluginCollector().collectDirtyRecord(record, e);
                    if (retryTimes++ > maxRetryTimes) {
                        throw DataXException.asDataXException(HbaseReaderErrorCode.READ_ERROR, "read from obhbase failed", e);
                    }
                    record = recordSender.createRecord();
                    continue;
                }
                if (fetchOK) {
                    recordSender.sendToWriter(record);
                    record = recordSender.createRecord();
                } else {
                    break;
                }
            }
            recordSender.flush();
        }
    }
}
