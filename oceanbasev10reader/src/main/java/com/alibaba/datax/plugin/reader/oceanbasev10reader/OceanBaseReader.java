package com.alibaba.datax.plugin.reader.oceanbasev10reader;

import java.sql.Connection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.ext.ReaderJob;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.ext.ReaderTask;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.ObReaderUtils;

public class OceanBaseReader extends Reader {

    public static class Job extends Reader.Job {
        private Configuration originalConfig = null;
        private ReaderJob readerJob;
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            Integer userConfigedFetchSize = this.originalConfig.getInt(Constant.FETCH_SIZE);
            if (userConfigedFetchSize != null) {
                LOG.warn("The [fetchSize] is not recognized, please use readBatchSize instead.");
            }
            this.originalConfig.set(Constant.FETCH_SIZE, Integer.MIN_VALUE);
            setDatabaseType(originalConfig);
            this.readerJob = new ReaderJob();
            this.readerJob.init(this.originalConfig);
        }

        @Override
        public void prepare() {
            //ObReaderUtils.DATABASE_TYPE获取当前数据库的语法模式
        }

        @Override
        public void preCheck() {
            init();
            this.readerJob.preCheck(this.originalConfig, ObReaderUtils.databaseType);

        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return this.readerJob.split(this.originalConfig, adviceNumber);
        }

        @Override
        public void post() {
            this.readerJob.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.readerJob.destroy(this.originalConfig);
        }

        private void setDatabaseType(Configuration config) {
            String username = config.getString(Key.USERNAME);
            String password = config.getString(Key.PASSWORD);
            List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
            Configuration connConf = Configuration.from(conns.get(0).toString());
            List<String> jdbcUrls = connConf.getList(Key.JDBC_URL, String.class);
            String jdbcUrl = jdbcUrls.get(0);
            if (jdbcUrl.startsWith(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING)) {
                String[] ss = jdbcUrl.split(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING_PATTERN);
                if (ss.length != 3) {
                    LOG.warn("unrecognized jdbc url: " + jdbcUrl);
                    return;
                }
                username = ss[1].trim() + ":" + username;
                jdbcUrl = ss[2];
            }
            // Use ob-client to get compatible mode.
            try {
                String obJdbcUrl = jdbcUrl.replace("jdbc:mysql:", "jdbc:oceanbase:");
                Connection conn = DBUtil.getConnection(DataBaseType.OceanBase, obJdbcUrl, username, password);
                String compatibleMode = ObReaderUtils.getCompatibleMode(conn);
                if (ObReaderUtils.isOracleMode(compatibleMode)) {
                    ObReaderUtils.compatibleMode = ObReaderUtils.OB_COMPATIBLE_MODE_ORACLE;
                }

            } catch (Exception e) {
                LOG.warn("error in get compatible mode, using mysql as default: " + e.getMessage());
            }
        }
    }

    public static class Task extends Reader.Task {
        private Configuration readerSliceConfig;
        private ReaderTask commonRdbmsReaderTask;
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsReaderTask = new ReaderTask(super.getTaskGroupId(), super.getTaskId());
            this.commonRdbmsReaderTask.init(this.readerSliceConfig);

        }

        @Override
        public void startRead(RecordSender recordSender) {
            int fetchSize = this.readerSliceConfig.getInt(Constant.FETCH_SIZE);
            this.commonRdbmsReaderTask.startRead(this.readerSliceConfig, recordSender, super.getTaskPluginCollector(),
                    fetchSize);
        }

        @Override
        public void post() {
            this.commonRdbmsReaderTask.post(this.readerSliceConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderTask.destroy(this.readerSliceConfig);
        }
    }

}
