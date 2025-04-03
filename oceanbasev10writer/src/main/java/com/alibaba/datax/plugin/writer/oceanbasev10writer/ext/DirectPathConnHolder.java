package com.alibaba.datax.plugin.writer.oceanbasev10writer.ext;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.Config;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.common.Table;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.directPath.DirectPathConnection;

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObLoadDupActionType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectPathConnHolder extends AbstractConnHolder {
    private static final Logger LOG = LoggerFactory.getLogger(DirectPathConnHolder.class);

    /**
     * The server side timeout.
     */
    private static final long SERVER_TIMEOUT = 24L * 60 * 60 * 1000 * 1000;

    private static final ConcurrentHashMap<Table, DirectPathConnection> cache = new ConcurrentHashMap<>();

    private String tableName;
    private String host;
    private int rpcPort;
    private String tenantName;
    private String databaseName;
    private int blocks;
    private int threads;
    private int maxErrors;
    private ObLoadDupActionType duplicateKeyAction;

    public DirectPathConnHolder(Configuration config, ServerConnectInfo connectInfo, String tableName, int threadsPerChannel) {
        super(config, connectInfo.jdbcUrl, connectInfo.userName, connectInfo.password);
        // direct path:
        //● publicCloud & odp - single or full
        //● publicCloud & observer - not support
        //● !publicCloud & odp - full
        //● !publicCloud & observer - single
        this.userName = connectInfo.getFullUserName();
        this.host = connectInfo.host;
        this.rpcPort = connectInfo.rpcPort;
        this.tenantName = connectInfo.tenantName;
        if (!connectInfo.publicCloud && StringUtils.isEmpty(tenantName)) {
            throw new IllegalStateException("tenant name is needed when using direct path load in private cloud.");
        }
        this.databaseName = connectInfo.databaseName;
        this.tableName = tableName;
        this.blocks = config.getInt(Config.BLOCKS_COUNT, 1);
        this.threads = threadsPerChannel * Math.min(blocks, 32);
        this.maxErrors = config.getInt(Config.MAX_ERRORS, 0);
        this.duplicateKeyAction = "insert".equalsIgnoreCase(config.getString(Config.OB_WRITE_MODE)) ? ObLoadDupActionType.IGNORE : ObLoadDupActionType.REPLACE;
    }

    @Override
    public Connection initConnection() {
        synchronized (cache) {
            conn = cache.computeIfAbsent(new Table(databaseName, tableName), e -> {
                try {
                    return new DirectPathConnection.Builder().host(host) //
                            .port(rpcPort) //
                            .tenant(tenantName) //
                            .user(userName) //
                            .password(Optional.ofNullable(password).orElse("")) //
                            .schema(databaseName) //
                            .table(tableName) //
                            .blocks(blocks) //
                            .parallel(threads) //
                            .maxErrorCount(maxErrors) //
                            .duplicateKeyAction(duplicateKeyAction) //
                            .serverTimeout(SERVER_TIMEOUT) //
                            .configuration(config)
                            .build();
                } catch (Exception ex) {
                    throw DataXException.asDataXException(DBUtilErrorCode.CONN_DB_ERROR, ex);
                }
            });
        }
        return conn;
    }

    public String getJdbcUrl() {
        return "";
    }

    public String getUserName() {
        return "";
    }

    @Override
    public void destroy() {
        if (conn != null && ((DirectPathConnection) conn).isFinished()) {
            DBUtil.closeDBResources(null, conn);
        }
    }

    @Override
    public void doCommit() {
        try {
            if (conn != null) {
                conn.commit();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
