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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Key implements Serializable {
    public static final String FE_LOAD_URL = "feLoadUrl";
    public static final String BE_LOAD_URL = "beLoadUrl";
    public static final String JDBC_URL = "jdbcUrl";

    public static final String DATABASE = "database";
    public static final String TABLE = "table";
    public static final String COLUMN = "column";
    public static final String TIME_ZONE = "timeZone";

    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";

    public static final String PRE_SQL = "preSql";
    public static final String POST_SQL = "postSql";

    public static final String LOAD_PROPS = "loadProps";
    public static final String LOAD_PROPS_LINE_DELIMITER = "line_delimiter";
    public static final String LOAD_PROPS_COLUMN_SEPARATOR = "column_separator";

    public static final String MAX_BATCH_ROWS = "maxBatchRows";
    public static final String MAX_BATCH_BYTE_SIZE = "maxBatchByteSize";
    public static final String MAX_RETRIES = "maxRetries";
    public static final String LABEL_PREFIX = "labelPrefix";
    public static final String FORMAT = "format";
    public static final String CONNECT_TIMEOUT = "connectTimeout";
    private final Configuration options;

    private static final long DEFAULT_MAX_BATCH_ROWS = 50_0000;
    private static final long DEFAULT_MAX_BATCH_BYTE_SIZE = 90 * 1024 * 1024; // 90MB
    private static final int DEFAULT_MAX_RETRIES = 0;

    private static final String DEFAULT_LABEL_PREFIX = "datax_doris_writer_";
    private static final String DEFAULT_COLUMN_SEPARATOR = "\\x01";
    private static final String DEFAULT_LINE_DELIMITER = "\\x02";
    public static final String DEFAULT_FORMAT_CSV = "csv";
    private static final String DEFAULT_TIME_ZONE = "+08:00";
    private static final int DEFAULT_CONNECT_TIMEOUT = -1;

    public Key(final Configuration options) {
        this.options = options;
    }

    public void doPretreatment() {
        this.validateRequired();
        this.validateStreamLoadUrl();
        this.validateFormat();
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

    public String getTimeZone() {
        return this.options.getString(TIME_ZONE, DEFAULT_TIME_ZONE);
    }

    public List<String> getPreSqlList() {
        return this.options.getList(PRE_SQL, String.class);
    }

    public List<String> getPostSqlList() {
        return this.options.getList(POST_SQL, String.class);
    }

    public Map<String, Object> getLoadProps() {
        return this.options.getMap(LOAD_PROPS, new HashMap<>());
    }

    public long getBatchRows() {
        return this.options.getLong(MAX_BATCH_ROWS, DEFAULT_MAX_BATCH_ROWS);
    }

    public long getBatchByteSize() {
        return this.options.getLong(MAX_BATCH_BYTE_SIZE, DEFAULT_MAX_BATCH_BYTE_SIZE);
    }

    public int getMaxRetries() {
        return this.options.getInt(MAX_RETRIES, DEFAULT_MAX_RETRIES);
    }

    public String getLabelPrefix() {
        return this.options.getString(LABEL_PREFIX, DEFAULT_LABEL_PREFIX);
    }

    public String getLineDelimiter() {
        return getLoadProps().getOrDefault(LOAD_PROPS_LINE_DELIMITER, DEFAULT_LINE_DELIMITER).toString();
    }

    public String getFormat() {
        return this.options.getString(FORMAT, DEFAULT_FORMAT_CSV);
    }

    public String getColumnSeparator() {
        return getLoadProps().getOrDefault(LOAD_PROPS_COLUMN_SEPARATOR, DEFAULT_COLUMN_SEPARATOR).toString();
    }

    public int getConnectTimeout() {
        return this.options.getInt(CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT);
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

    private void validateFormat() {
        String format = this.getFormat();
        if (!Arrays.asList("csv", "json").contains(format.toLowerCase())) {
            throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR, "format only supports csv or json");
        }
    }

    private void validateRequired() {
        final String[] requiredOptionKeys = new String[]{JDBC_URL, USERNAME, DATABASE, TABLE, COLUMN};
        for (final String optionKey : requiredOptionKeys) {
            this.options.getNecessaryValue(optionKey, DBUtilErrorCode.REQUIRED_VALUE);
        }
    }
}
