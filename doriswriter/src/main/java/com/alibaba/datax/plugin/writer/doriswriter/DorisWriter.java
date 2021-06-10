package com.alibaba.datax.plugin.writer.doriswriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.druid.sql.parser.ParserException;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DorisWriter extends Writer {
    public DorisWriter() {
    }

    public static class Task extends com.alibaba.datax.common.spi.Writer.Task {
        private DorisWriterManager writerManager;
        private Key keys;
        private DorisJsonCodec rowCodec;

        public Task() {
        }

        @Override
        public void init() {
            this.keys = new Key(super.getPluginJobConf());
            this.writerManager = new DorisWriterManager(this.keys);
            this.rowCodec = new DorisJsonCodec(this.keys.getColumns());
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            try {
                Record record;
                while((record = recordReceiver.getFromReader()) != null) {
                    if (record.getColumnNumber() != this.keys.getColumns().size()) {
                        throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                                String.format("列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 请检查您的配置并作出修改.", record.getColumnNumber(), this.keys.getColumns().size()));
                    }

                    this.writerManager.writeRecord(this.rowCodec.serialize(record));
                }

            } catch (Exception e) {
                throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
            }
        }

        @Override
        public void post() {
            try {
                this.writerManager.close();
            } catch (Exception e) {
                throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
            }
        }

        @Override
        public void destroy() {
        }

        @Override
        public boolean supportFailOver() {
            return false;
        }
    }

    public static class Job extends com.alibaba.datax.common.spi.Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(DorisWriter.Job.class);
        private Configuration originalConfig = null;
        private Key keys;

        public Job() {
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.keys = new Key(super.getPluginJobConf());
            this.keys.doPretreatment();
        }

        @Override
        public void preCheck() {
            this.init();
            this.preCheckPrePareSQL(this.keys);
            this.preCheckPostSQL(this.keys);
        }

        @Override
        public void prepare() {
            String username = this.keys.getUsername();
            String password = this.keys.getPassword();
            String jdbcUrl = this.keys.getJdbcUrl();
            List<String> renderedPreSqls = this.renderPreOrPostSqls(this.keys.getPreSqlList(), this.keys.getTable());
            if (!renderedPreSqls.isEmpty()) {
                Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl, username, password);
                LOG.info("开始执行 preSqls:[{}]. doris jdbc url为:{}.", String.join(";", renderedPreSqls), jdbcUrl);
                this.executeSqls(conn, renderedPreSqls);
                DBUtil.closeDBResources(null, null, conn);
            }

        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configurations = new ArrayList<>(mandatoryNumber);

            for(int i = 0; i < mandatoryNumber; ++i) {
                configurations.add(this.originalConfig);
            }

            return configurations;
        }

        @Override
        public void post() {
            String username = this.keys.getUsername();
            String password = this.keys.getPassword();
            String jdbcUrl = this.keys.getJdbcUrl();
            List<String> renderedPostSqls = this.renderPreOrPostSqls(this.keys.getPostSqlList(), this.keys.getTable());
            if (!renderedPostSqls.isEmpty()) {
                Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl, username, password);
                LOG.info("开始执行 preSqls:[{}]. doris jdbc url为:{}.", String.join(";", renderedPostSqls), jdbcUrl);
                this.executeSqls(conn, renderedPostSqls);
                DBUtil.closeDBResources(null, null, conn);
            }

        }

        @Override
        public void destroy() {
        }

        private List<String> renderPreOrPostSqls(final List<String> preOrPostSqls, final String tableName) {
            if (null == preOrPostSqls) {
                return Collections.emptyList();
            }
            final List<String> renderedSqls = new ArrayList<>();
            for (final String sql : preOrPostSqls) {
                if (!Strings.isNullOrEmpty(sql)) {
                    renderedSqls.add(sql.replace(Constant.TABLE_NAME_PLACEHOLDER, tableName));
                }
            }
            return renderedSqls;
        }

        private void executeSqls(final Connection conn, final List<String> sqls) {
            Statement stmt = null;
            String currentSql = null;
            try {
                stmt = conn.createStatement();
                for (String s : sqls) {
                    final String sql = currentSql = s;
                    DBUtil.executeSqlWithoutResultSet(stmt, sql);
                }
            } catch (Exception e) {
                throw RdbmsException.asQueryException(DataBaseType.MySql, e, currentSql, null, null);
            } finally {
                DBUtil.closeDBResources(null, stmt, null);
            }
        }

        private void preCheckPrePareSQL(final Key keys) {
            final String table = keys.getTable();
            final List<String> preSqls = keys.getPreSqlList();
            final List<String> renderedPreSqls = renderPreOrPostSqls(preSqls, table);
            if (!renderedPreSqls.isEmpty()) {
                LOG.info("开始检查 preSqls:[{}].", String.join(";", renderedPreSqls));
                for (final String sql : renderedPreSqls) {
                    try {
                        DBUtil.sqlValid(sql, DataBaseType.MySql);
                    } catch (ParserException e) {
                        throw RdbmsException.asPreSQLParserException(DataBaseType.MySql, e, sql);
                    }
                }
            }
        }

        private void preCheckPostSQL(final Key keys) {
            final String table = keys.getTable();
            final List<String> postSqls = keys.getPostSqlList();
            final List<String> renderedPostSqls = renderPreOrPostSqls(postSqls, table);
            if (!renderedPostSqls.isEmpty()) {
                LOG.info("开始检查 postSqls:[{}].", String.join(";", renderedPostSqls));
                for (final String sql : renderedPostSqls) {
                    try {
                        DBUtil.sqlValid(sql, DataBaseType.MySql);
                    } catch (ParserException e) {
                        throw RdbmsException.asPostSQLParserException(DataBaseType.MySql, e, sql);
                    }
                }
            }
        }

    }
}