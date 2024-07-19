package com.alibaba.datax.plugin.writer.obhbasewriter;

import static com.alibaba.datax.plugin.writer.obhbasewriter.Constant.DEFAULT_OB_TABLE_CLIENT_LOG_LEVEL;
import static com.alibaba.datax.plugin.writer.obhbasewriter.Constant.DEFAULT_OB_TABLE_HBASE_LOG_LEVEL;
import static com.alibaba.datax.plugin.writer.obhbasewriter.Constant.OB_COM_ALIPAY_TABLE_CLIENT_LOG_LEVEL;
import static com.alibaba.datax.plugin.writer.obhbasewriter.Constant.OB_COM_ALIPAY_TABLE_HBASE_LOG_LEVEL;
import static com.alibaba.datax.plugin.writer.obhbasewriter.Constant.OB_HBASE_LOG_PATH;
import static com.alibaba.datax.plugin.writer.obhbasewriter.Constant.OB_TABLE_CLIENT_LOG_LEVEL;
import static com.alibaba.datax.plugin.writer.obhbasewriter.Constant.OB_TABLE_CLIENT_PROPERTY;
import static com.alibaba.datax.plugin.writer.obhbasewriter.Constant.OB_TABLE_HBASE_LOG_LEVEL;
import static com.alibaba.datax.plugin.writer.obhbasewriter.Constant.OB_TABLE_HBASE_PROPERTY;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.util.ObVersion;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.datax.plugin.writer.obhbasewriter.ext.ServerConnectInfo;
import com.alibaba.datax.plugin.writer.obhbasewriter.task.ObHBaseWriteTask;
import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 *
 */
public class ObHbaseWriter extends Writer {
    /**
     * Job 中的方法仅执行一次，Task 中方法会由框架启动多个 Task 线程并行执行。
     * <p/>
     * 整个 Writer 执行流程是：
     *
     * <pre>
     * Job类init-->prepare-->split
     *
     *                          Task类init-->prepare-->startWrite-->post-->destroy
     *                          Task类init-->prepare-->startWrite-->post-->destroy
     *
     *                                                                            Job类post-->destroy
     * </pre>
     */
    public static class Job extends Writer.Job {
        private Configuration originalConfig = null;
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        /**
         * 注意：此方法仅执行一次。 最佳实践：通常在这里对用户的配置进行校验：是否缺失必填项？有无错误值？有没有无关配置项？...
         * 并给出清晰的报错/警告提示。校验通常建议采用静态工具类进行，以保证本类结构清晰。
         */
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
            boolean useOdpMode = originalConfig.getBool(ConfigKey.USE_ODP_MODE, false);
            String configUrl = originalConfig.getString(ConfigKey.OBCONFIG_URL, null);
            String jdbcUrl = originalConfig.getString(ConfigKey.JDBC_URL, null);
            jdbcUrl = DataBaseType.MySql.appendJDBCSuffixForReader(jdbcUrl);
            String user = originalConfig.getString(Key.USERNAME, null);
            String password = originalConfig.getString(Key.PASSWORD);
            ServerConnectInfo serverConnectInfo = new ServerConnectInfo(jdbcUrl, user, password);
            if (useOdpMode) {
                originalConfig.set(ConfigKey.ODP_HOST, serverConnectInfo.host);
                originalConfig.set(ConfigKey.ODP_PORT, serverConnectInfo.port);
            } else if (StringUtils.isBlank(configUrl)) {
                serverConnectInfo.setSysUser(originalConfig.getString(ConfigKey.OB_SYS_USER));
                serverConnectInfo.setSysPass(originalConfig.getString(ConfigKey.OB_SYS_PASSWORD));
                try {
                    originalConfig.set(ConfigKey.OBCONFIG_URL, queryRsUrl(serverConnectInfo));
                    originalConfig.set(ConfigKey.OB_SYS_USER, serverConnectInfo.sysUser);
                    originalConfig.set(ConfigKey.OB_SYS_PASSWORD, serverConnectInfo.sysPass);
                    LOG.info("fetch configUrl success, configUrl is {}", configUrl);
                } catch (Exception e) {
                    LOG.error("fail to get configure url: " + e.getMessage());
                    throw DataXException.asDataXException(Hbase094xWriterErrorCode.REQUIRED_VALUE, "Missing obConfigUrl");
                }
            }
            if (StringUtils.isBlank(originalConfig.getString(ConfigKey.DBNAME))) {
                originalConfig.set(ConfigKey.DBNAME, serverConnectInfo.databaseName);
            }
            ConfigValidator.validateParameter(this.originalConfig);
        }

        private String queryRsUrl(ServerConnectInfo serverInfo) {
            String configUrl = originalConfig.getString(ConfigKey.OBCONFIG_URL, null);
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
                    originalConfig.set(ConfigKey.OBCONFIG_URL, configUrl);
                } catch (Exception e) {
                    LOG.error("Fail to get configure url: {}", e.getMessage(), e);
                    throw DataXException.asDataXException(Hbase094xWriterErrorCode.REQUIRED_VALUE, "未配置obConfigUrl，且无法获取obConfigUrl");
                }
            }
            return configUrl;
        }

        /**
         * 注意：此方法仅执行一次。 最佳实践：如果 Job 中有需要进行数据同步之前的处理，可以在此处完成，如果没有必要则可以直接去掉。
         */
        // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
        @Override
        public void prepare() {
        }

        /**
         * 注意：此方法仅执行一次。 最佳实践：通常采用工具静态类完成把 Job 配置切分成多个 Task 配置的工作。 这里的
         * mandatoryNumber 是强制必须切分的份数。
         */
        @Override
        public List<Configuration> split(int mandatoryNumber) {
            // This function does not need any change.
            Configuration simplifiedConf = this.originalConfig;

            List<Configuration> splitResultConfigs = new ArrayList<Configuration>();
            for (int j = 0; j < mandatoryNumber; j++) {
                splitResultConfigs.add(simplifiedConf.clone());
            }
            return splitResultConfigs;
        }

        /**
         * 注意：此方法仅执行一次。 最佳实践：如果 Job 中有需要进行数据同步之后的后续处理，可以在此处完成。
         */
        @Override
        public void post() {
            // No post supported
        }

        /**
         * 注意：此方法仅执行一次。 最佳实践：通常配合 Job 中的 post() 方法一起完成 Job 的资源释放。
         */
        @Override
        public void destroy() {

        }
    }

    public static class Task extends Writer.Task {
        private Configuration taskConfig;
        private CommonRdbmsWriter.Task writerTask;

        /**
         * 注意：此方法每个 Task 都会执行一次。 最佳实践：此处通过对 taskConfig 配置的读取，进而初始化一些资源为
         * startWrite()做准备。
         */
        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();
            String mode = this.taskConfig.getString(ConfigKey.MODE);
            ModeType modeType = ModeType.getByTypeName(mode);

            switch (modeType) {
                case Normal:
                    try {
                        this.writerTask = new ObHBaseWriteTask(this.taskConfig);
                    } catch (Exception e) {
                        throw DataXException.asDataXException(Hbase094xWriterErrorCode.INIT_ERROR, "ObHbase writer init error:" + e.getMessage());
                    }
                    break;
                default:
                    throw DataXException.asDataXException(Hbase094xWriterErrorCode.ILLEGAL_VALUE, "ObHbase not support this mode type:" + modeType);
            }
        }

        /**
         * 注意：此方法每个 Task 都会执行一次。 最佳实践：如果 Task
         * 中有需要进行数据同步之前的处理，可以在此处完成，如果没有必要则可以直接去掉。
         */
        @Override
        public void prepare() {
            this.writerTask.prepare(taskConfig);
        }

        /**
         * 注意：此方法每个 Task 都会执行一次。 最佳实践：此处适当封装确保简洁清晰完成数据写入工作。
         */
        public void startWrite(RecordReceiver recordReceiver) {
            this.writerTask.startWrite(recordReceiver, taskConfig, super.getTaskPluginCollector());
        }

        /**
         * 注意：此方法每个 Task 都会执行一次。 最佳实践：如果 Task 中有需要进行数据同步之后的后续处理，可以在此处完成。
         */
        @Override
        public void post() {
            this.writerTask.post(taskConfig);
        }

        /**
         * 注意：此方法每个 Task 都会执行一次。 最佳实践：通常配合Task 中的 post() 方法一起完成 Task 的资源释放。
         */
        @Override
        public void destroy() {
            this.writerTask.destroy(taskConfig);
        }
    }
}
