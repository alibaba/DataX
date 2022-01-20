// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
import java.util.UUID;

public class DorisWriter extends Writer {
    public DorisWriter() {
    }

    public static class Task extends com.alibaba.datax.common.spi.Writer.Task {
        private DorisWriterEmitter dorisWriterEmitter;
        private Key keys;
        private DorisCodec rowCodec;
        private int batchNum = 0;

        public Task() {
        }

        @Override
        public void init() {
            this.keys = new Key(super.getPluginJobConf());
            this.rowCodec = new DorisJsonCodec(this.keys.getColumns());
            this.dorisWriterEmitter = new DorisWriterEmitter(keys);
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            String lineDelimiter = this.keys.getLineDelimiter();
            DorisFlushBatch flushBatch = new DorisFlushBatch(lineDelimiter);
            long batchCount = 0;
            long batchByteSize = 0L;
            Record record;
            // loop to get record from datax
            while ((record = recordReceiver.getFromReader()) != null) {
                // check column size
                if (record.getColumnNumber() != this.keys.getColumns().size()) {
                    throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                            String.format("config writer column info error. because the column number of reader is :%s" +
                                            "and the column number of writer is:%s. please check you datax job config json.",
                                    record.getColumnNumber(), this.keys.getColumns().size()));
                }
                // codec record
                final String recordStr = this.rowCodec.serialize(record);

                // put into buffer
                flushBatch.putData(recordStr);
                batchCount += 1;
                batchByteSize += recordStr.length();
                // trigger buffer
                if (batchCount >= this.keys.getBatchRows() || batchByteSize >= this.keys.getBatchByteSize()) {
                    // generate doris stream load label
                    flush(flushBatch);
                    // clear buffer
                    batchCount = 0;
                    batchByteSize = 0L;
                    flushBatch = new DorisFlushBatch(lineDelimiter);
                }
            } // end of while

            if (flushBatch.getSize() > 0) {
                flush(flushBatch);
            }
        }

        private void flush(DorisFlushBatch flushBatch) {
            final String label = getStreamLoadLabel();
            flushBatch.setLabel(label);
            dorisWriterEmitter.doStreamLoad(flushBatch);
        }

        private String getStreamLoadLabel() {
            String labelPrefix = this.keys.getLabelPrefix();
            return labelPrefix + UUID.randomUUID().toString() + "_" + (batchNum++);
        }

        @Override
        public void post() {

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
                LOG.info("prepare execute preSqls:[{}]. doris jdbc url:{}.", String.join(";", renderedPreSqls), jdbcUrl);
                this.executeSqls(conn, renderedPreSqls);
                DBUtil.closeDBResources(null, null, conn);
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configurations = new ArrayList<>(mandatoryNumber);

            for (int i = 0; i < mandatoryNumber; ++i) {
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
                LOG.info("prepare execute postSqls:[{}]. doris jdbc url为:{}.", String.join(";", renderedPostSqls), jdbcUrl);
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
                LOG.info("prepare check preSqls:[{}].", String.join(";", renderedPreSqls));
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
                LOG.info("prepare check postSqls:[{}].", String.join(";", renderedPostSqls));
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
