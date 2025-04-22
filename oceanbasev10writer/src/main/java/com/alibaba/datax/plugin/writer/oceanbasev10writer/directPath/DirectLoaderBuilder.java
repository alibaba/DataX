/*
 * Copyright 2024 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.datax.plugin.writer.oceanbasev10writer.directPath;

import java.io.Serializable;

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadConnection;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadManager;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadStatement;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadException;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObLoadDupActionType;
import org.apache.commons.lang.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The builder for {@link ObTableDirectLoad}.
 */
public class DirectLoaderBuilder implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(DirectLoaderBuilder.class);
    private String host;
    private int port;

    private String user;
    private String tenant;
    private String password;

    private String schema;
    private String table;

    /**
     * Server-side parallelism.
     */
    private int parallel;

    private long maxErrorCount;

    private ObLoadDupActionType duplicateKeyAction;

    /**
     * The overall timeout of the direct load task
     */
    private Long timeout;

    private Long heartBeatTimeout;

    private Long heartBeatInterval;

    public DirectLoaderBuilder host(String host) {
        this.host = host;
        return this;
    }

    public DirectLoaderBuilder port(int port) {
        this.port = port;
        return this;
    }

    public DirectLoaderBuilder user(String user) {
        //1.4.0的obkv版本只需要用户名称，不能带租户和集群信息
        int indexOf = user.indexOf("@");
        this.user = user;
        if (indexOf > 0) {
            this.user = user.substring(0, indexOf);
        }
        return this;
    }

    public DirectLoaderBuilder tenant(String tenant) {
        this.tenant = tenant;
        return this;
    }

    public DirectLoaderBuilder password(String password) {
        this.password = password;
        return this;
    }

    public DirectLoaderBuilder schema(String schema) {
        this.schema = schema;
        return this;
    }

    public DirectLoaderBuilder table(String table) {
        this.table = table;
        return this;
    }

    public DirectLoaderBuilder parallel(int parallel) {
        this.parallel = parallel;
        return this;
    }

    public DirectLoaderBuilder maxErrorCount(long maxErrorCount) {
        this.maxErrorCount = maxErrorCount;
        return this;
    }

    public DirectLoaderBuilder duplicateKeyAction(ObLoadDupActionType duplicateKeyAction) {
        this.duplicateKeyAction = duplicateKeyAction;
        return this;
    }

    public DirectLoaderBuilder timeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

    public DirectLoaderBuilder heartBeatTimeout(Long heartBeatTimeout) {
        this.heartBeatTimeout = heartBeatTimeout;
        return this;
    }

    public DirectLoaderBuilder heartBeatInterval(Long heartBeatInterval) {
        this.heartBeatInterval = heartBeatInterval;
        return this;
    }

    public ObTableDirectLoad build() {
        try {
            ObDirectLoadConnection obDirectLoadConnection = buildConnection(parallel);
            ObDirectLoadStatement obDirectLoadStatement = buildStatement(obDirectLoadConnection);
            return new ObTableDirectLoad(schema, table, obDirectLoadStatement, obDirectLoadConnection);
        } catch (ObDirectLoadException e) {
            throw new ObTableException(e.getMessage(), e);
        }
    }

    private ObDirectLoadConnection buildConnection(int writeThreadNum) throws ObDirectLoadException {
        if (heartBeatTimeout == null || heartBeatInterval == null) {
            throw new IllegalArgumentException("heartBeatTimeout and heartBeatInterval must not be null");
        }
        ObDirectLoadConnection build = ObDirectLoadManager.getConnectionBuilder()
                .setServerInfo(host, port)
                .setLoginInfo(tenant, user, password, schema)
                .setHeartBeatInfo(heartBeatTimeout, heartBeatInterval)
                .enableParallelWrite(writeThreadNum)
                .build();
        log.info("ObDirectLoadConnection value is:{}", ObjectUtils.toString(build));
        return build;
    }

    private ObDirectLoadStatement buildStatement(ObDirectLoadConnection connection) throws ObDirectLoadException {
        ObDirectLoadStatement build = connection.getStatementBuilder()
                .setTableName(table)
                .setParallel(parallel)
                .setQueryTimeout(timeout)
                .setDupAction(duplicateKeyAction)
                .setMaxErrorRowCount(maxErrorCount)
                .build();
        log.info("ObDirectLoadStatement value is:{}", ObjectUtils.toString(build));
        return build;
    }
}
