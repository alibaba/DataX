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

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.google.common.base.Strings;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class Key implements Serializable {
    public static final String FE_LOAD_URL = "feLoadUrl";
    public static final String BE_LOAD_URL = "beLoadUrl";
    public static final String JDBC_URL = "jdbcUrl";

    public static final String DATABASE = "database";
    public static final String TABLE = "table";
    public static final String COLUMN = "column";

    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";

    public static final String PRE_SQL = "preSql";
    public static final String POST_SQL = "postSql";

    public static final String LOAD_PROPS = "loadProps";
    public static final String MAX_BATCH_ROWS = "maxBatchRows";
    public static final String MAX_BATCH_BYTE_SIZE = "maxBatchByteSize";
    public static final String LABEL_PREFIX = "labelPrefix";
    public static final String LINE_DELIMITER = "lineDelimiter";
    public static final String CONNECT_TIMEOUT = "connectTimeout";
    private final Configuration options;
    private final String lineDelimiterDesc;

    private static final long DEFAULT_MAX_BATCH_ROWS = 50_0000;
    private static final long DEFAULT_MAX_BATCH_BYTE_SIZE = 100 * 1024 * 1024; // 100MB
    private static final String DEFAULT_LABEL_PREFIX = "datax_doris_writer_";
    private static final String DEFAULT_LINE_DELIMITER = "\n";
    private static final int DEFAULT_CONNECT_TIMEOUT = -1;

    public Key(final Configuration options) {
        this.options = options;
        this.lineDelimiterDesc = parseHexReadable(this.getLineDelimiter());
    }

    public void doPretreatment() {
        this.validateRequired();
        this.validateStreamLoadUrl();
    }

    public String getJdbcUrl() {
        return this.options.getString(JDBC_URL);
    }

    public String getDatabase() {
        return this.options.getString(DATABASE);
    }

    public String getTable() {
        return this.options.getString(TABLE);
    }

    public String getUsername() {
        return this.options.getString(USERNAME);
    }

    public String getPassword() {
        return Strings.nullToEmpty(this.options.getString(PASSWORD));
    }

    public List<String> getBeLoadUrlList() {
        return this.options.getList(BE_LOAD_URL, String.class);
    }

    public List<String> getFeLoadUrlList() {
        return this.options.getList(FE_LOAD_URL, String.class);
    }

    public List<String> getColumns() {
        return this.options.getList(COLUMN, String.class);
    }

    public List<String> getPreSqlList() {
        return this.options.getList(PRE_SQL, String.class);
    }

    public List<String> getPostSqlList() {
        return this.options.getList(POST_SQL, String.class);
    }

    public Map<String, Object> getLoadProps() {
        return this.options.getMap(LOAD_PROPS);
    }

    public long getBatchRows() {
        return this.options.getLong(MAX_BATCH_ROWS, DEFAULT_MAX_BATCH_ROWS);
    }

    public long getBatchByteSize() {
        return this.options.getLong(MAX_BATCH_BYTE_SIZE, DEFAULT_MAX_BATCH_BYTE_SIZE);
    }

    public String getLabelPrefix() {
        return this.options.getString(LABEL_PREFIX, DEFAULT_LABEL_PREFIX);
    }

    public String getLineDelimiter() {
        return this.options.getString(LINE_DELIMITER, DEFAULT_LINE_DELIMITER);
    }

    public int getConnectTimeout() {
        return this.options.getInt(CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT);
    }

    public String getLineDelimiterDesc() {
        return lineDelimiterDesc;
    }

    private void validateStreamLoadUrl() {
        List<String> urlList = this.getBeLoadUrlList();
        if (urlList == null) {
            urlList = this.getFeLoadUrlList();
        }
        if (urlList == null || urlList.isEmpty()) {
            throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR, "Either beLoadUrl or feLoadUrl must be set");
        }

        for (final String host : urlList) {
            if (host.split(":").length < 2) {
                throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                        "Invalid load url format. IF use FE hosts, should be like: fe_host:fe_http_port."
                                + " If use BE hosts, should be like: be_host:be_webserver_port");
            }
        }
    }

    private String parseHexReadable(String s) {
        byte[] separatorBytes = s.getBytes();
        StringBuilder desc = new StringBuilder();

        for (byte separatorByte : separatorBytes) {
            desc.append(String.format("\\x%02x", separatorByte));
        }
        return desc.toString();
    }

    private void validateRequired() {
        final String[] requiredOptionKeys = new String[]{JDBC_URL, USERNAME, DATABASE, TABLE, COLUMN};
        for (final String optionKey : requiredOptionKeys) {
            this.options.getNecessaryValue(optionKey, DBUtilErrorCode.REQUIRED_VALUE);
        }
    }
}
