package com.starrocks.connector.datax.plugin.writer.starrockswriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.starrocks.connector.datax.plugin.writer.starrockswriter.manager.StarRocksWriterManager;
import com.starrocks.connector.datax.plugin.writer.starrockswriter.row.StarRocksISerializer;
import com.starrocks.connector.datax.plugin.writer.starrockswriter.row.StarRocksSerializerFactory;
import com.starrocks.connector.datax.plugin.writer.starrockswriter.util.StarRocksWriterUtil;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class DualStarRocksWriter extends Writer {

    public static class Job extends Writer.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration originalConfig = null;
        private StarRocksWriterOptions optionsA;
        private StarRocksWriterOptions optionsB;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            String selectedDatabase = super.getPluginJobConf().getString(StarRocksWriterOptions.KEY_SELECTED_DATABASE);
            if (StringUtils.isBlank(this.originalConfig.getString(StarRocksWriterOptions.KEY_DATABASE)) && StringUtils.isNotBlank(selectedDatabase)) {
                this.originalConfig.set(StarRocksWriterOptions.KEY_DATABASE, selectedDatabase);
            }
            optionsA = new StarRocksWriterOptions(super.getPluginJobConf().getConfiguration("clusterA"));
            optionsB = new StarRocksWriterOptions(super.getPluginJobConf().getConfiguration("clusterB"));
            optionsA.doPretreatment();
            optionsB.doPretreatment();
        }

        @Override
        public void preCheck() {
            this.init();
            StarRocksWriterUtil.preCheckPrePareSQL(optionsA);
            StarRocksWriterUtil.preCheckPrePareSQL(optionsB);
            StarRocksWriterUtil.preCheckPostSQL(optionsA);
            StarRocksWriterUtil.preCheckPostSQL(optionsB);
        }

        @Override
        public void prepare() {
            executePreSqls(optionsA);
            executePreSqls(optionsB);
        }

        private void executePreSqls(StarRocksWriterOptions options) {
            String username = options.getUsername();
            String password = options.getPassword();
            String jdbcUrl = options.getJdbcUrl();
            List<String> renderedPreSqls = StarRocksWriterUtil.renderPreOrPostSqls(options.getPreSqlList(), options.getTable());
            if (null != renderedPreSqls && !renderedPreSqls.isEmpty()) {
                Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl, username, password);
                LOG.info("Begin to execute preSqls:[{}]. context info:{}.", String.join(";", renderedPreSqls), jdbcUrl);
                StarRocksWriterUtil.executeSqls(conn, renderedPreSqls);
                DBUtil.closeDBResources(null, null, conn);
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configurations = new ArrayList<>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(originalConfig);
            }
            return configurations;
        }

        @Override
        public void post() {
            executePostSqls(optionsA);
            executePostSqls(optionsB);
        }

        private void executePostSqls(StarRocksWriterOptions options) {
            String username = options.getUsername();
            String password = options.getPassword();
            String jdbcUrl = options.getJdbcUrl();
            List<String> renderedPostSqls = StarRocksWriterUtil.renderPreOrPostSqls(options.getPostSqlList(), options.getTable());
            if (null != renderedPostSqls && !renderedPostSqls.isEmpty()) {
                Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl, username, password);
                LOG.info("Begin to execute postSqls:[{}]. context info:{}.", String.join(";", renderedPostSqls), jdbcUrl);
                StarRocksWriterUtil.executeSqls(conn, renderedPostSqls);
                DBUtil.closeDBResources(null, null, conn);
            }
        }

        @Override
        public void destroy() {
        }

    }

    public static class Task extends Writer.Task {
        private StarRocksWriterManager writerManagerA;
        private StarRocksWriterManager writerManagerB;
        private StarRocksWriterOptions optionsA;
        private StarRocksWriterOptions optionsB;
        private StarRocksISerializer rowSerializerA;
        private StarRocksISerializer rowSerializerB;

        @Override
        public void init() {
            optionsA = new StarRocksWriterOptions(super.getPluginJobConf().getConfiguration("clusterA"));
            optionsB = new StarRocksWriterOptions(super.getPluginJobConf().getConfiguration("clusterB"));
            if (optionsA.isWildcardColumn()) {
                Connection conn = DBUtil.getConnection(DataBaseType.MySql, optionsA.getJdbcUrl(), optionsA.getUsername(), optionsA.getPassword());
                List<String> columns = StarRocksWriterUtil.getStarRocksColumns(conn, optionsA.getDatabase(), optionsA.getTable());
                optionsA.setInfoCchemaColumns(columns);
            }
            if (optionsB.isWildcardColumn()) {
                Connection conn = DBUtil.getConnection(DataBaseType.MySql, optionsB.getJdbcUrl(), optionsB.getUsername(), optionsB.getPassword());
                List<String> columns = StarRocksWriterUtil.getStarRocksColumns(conn, optionsB.getDatabase(), optionsB.getTable());
                optionsB.setInfoCchemaColumns(columns);
            }
            writerManagerA = new StarRocksWriterManager(optionsA);
            writerManagerB = new StarRocksWriterManager(optionsB);
            rowSerializerA = StarRocksSerializerFactory.createSerializer(optionsA);
            rowSerializerB = StarRocksSerializerFactory.createSerializer(optionsB);
        }

        @Override
        public void prepare() {
        }

        public void startWrite(RecordReceiver recordReceiver) {
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    if (record.getColumnNumber() != optionsA.getColumns().size() || record.getColumnNumber() != optionsB.getColumns().size()) {
                        throw DataXException
                                .asDataXException(
                                        DBUtilErrorCode.CONF_ERROR,
                                        String.format(
                                                "Column configuration error. The number of reader columns %d and the number of writer columns for clusterA %d and clusterB %d are not equal.",
                                                record.getColumnNumber(),
                                                optionsA.getColumns().size(),
                                                optionsB.getColumns().size()));
                    }
                    writerManagerA.writeRecord(rowSerializerA.serialize(record));
                    writerManagerB.writeRecord(rowSerializerB.serialize(record));
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
            }
        }

        @Override
        public void post() {
            try {
                writerManagerA.close();
                writerManagerB.close();
            } catch (Exception e) {
                throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
            }
        }

        @Override
        public void destroy() {}

        @Override
        public boolean supportFailOver(){
            return false;
        }
    }
}
