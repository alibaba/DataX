package com.alibaba.datax.plugin.writer.oceanbasev10writer.directPath;

import java.sql.SQLException;
import java.util.Objects;

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadBucket;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadConnection;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadStatement;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadTraceId;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadException;
import com.alipay.oceanbase.rpc.direct_load.protocol.payload.ObTableLoadClientStatus;
import com.alipay.oceanbase.rpc.table.ObTable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper of the direct-load API for OceanBase.
 */
public class ObTableDirectLoad implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ObTableDirectLoad.class);

    private final String tableName;
    private final String schemaTableName;
    private final ObDirectLoadStatement statement;
    private final ObDirectLoadConnection connection;

    public ObTableDirectLoad(String schemaName, String tableName, ObDirectLoadStatement statement, ObDirectLoadConnection connection) {
        Objects.requireNonNull(schemaName, "schemaName must not be null");
        Objects.requireNonNull(tableName, "tableName must not be null");
        Objects.requireNonNull(statement, "statement must not be null");
        Objects.requireNonNull(connection, "connection must not be null");
        this.tableName = tableName;
        this.schemaTableName = String.format("%s.%s", schemaName, tableName);
        this.statement = statement;
        this.connection = connection;
    }

    /**
     * Begin the direct load operation.
     *
     * @throws ObDirectLoadException if an error occurs during the operation.
     */
    public void begin() throws ObDirectLoadException {
        statement.begin();
    }

    /**
     * Write data into the direct load operation.
     *
     * @param bucket The data bucket to write.
     * @throws SQLException if writing fails.
     */
    public void write(ObDirectLoadBucket bucket) throws SQLException {
        try {
            if (bucket == null || bucket.isEmpty()) {
                throw new IllegalArgumentException("Bucket must not be null or empty.");
            }
            LOG.info("Writing {} rows to table: {}", bucket.getRowNum(), schemaTableName);
            statement.write(bucket);
            LOG.info("Successfully wrote bucket data to table: {}", schemaTableName);
        } catch (ObDirectLoadException e) {
            LOG.error("Failed to write to table: {}", schemaTableName, e);
            throw new SQLException(String.format("Failed to write to table: %s", schemaTableName), e);
        }
    }

    /**
     * Commit the current direct load operation.
     *
     * @throws SQLException if commit fails.
     */
    public void commit() throws SQLException {
        try {
            LOG.info("Committing direct load for table: {}", schemaTableName);
            statement.commit();
            LOG.info("Successfully committed direct load for table: {}", schemaTableName);
        } catch (ObDirectLoadException e) {
            LOG.error("Failed to commit for table: {}", schemaTableName, e);
            throw new SQLException(String.format("Failed to commit for table: %s", schemaTableName), e);
        }
    }

    /**
     * Close the direct load operation.
     */
    public void close() {
        LOG.info("Closing direct load for table: {}", schemaTableName);
        statement.close();
        connection.close();
        LOG.info("Direct load closed for table: {}", schemaTableName);
    }

    /**
     * Gets the status from the current connection based on the traceId
     */
    public ObTableLoadClientStatus getStatus() throws SQLException {
        ObDirectLoadTraceId traceId = statement.getTraceId();
        // Check if traceId is null and throw an exception with a clear message
        if (traceId == null) {
            throw new SQLException("traceId is null.");
        }
        // Retrieve the status using the traceId
        ObTableLoadClientStatus status = statement.getConnection().getProtocol().getHeartBeatRpc(traceId).getStatus();
        if (status == null) {
            LOG.info("Direct load connect protocol heartBeatRpc for table is null: {}", schemaTableName);
            throw new SQLException("status is null.");
        }
        // Return status if not null; otherwise, return ERROR
        return status;
    }

    /**
     * Gets the current table
     */
    public ObTable getTable() {
        try {
            return this.statement.getObTablePool().getControlObTable();
        } catch (ObDirectLoadException e) {
            throw new RuntimeException(e);
        }
    }

    public String getTableName() {
        if (StringUtils.isBlank(tableName)) {
            throw new IllegalArgumentException("tableName is blank.");
        }
        return tableName;
    }

    /**
     * Inserts data into the direct load operation.
     *
     * @param bucket The data bucket containing rows to insert.
     * @throws SQLException if an error occurs during the insert operation.
     */
    public void insert(ObDirectLoadBucket bucket) throws SQLException {
        LOG.info("Inserting {} rows to table: {}", bucket.getRowNum(), schemaTableName);

        if (bucket.isEmpty()) {
            LOG.warn("Parameter 'bucket' must not be empty.");
            throw new IllegalArgumentException("Parameter 'bucket' must not be empty.");
        }

        try {
            // Perform the insertion into the load operation
            statement.write(bucket);
            LOG.info("Successfully inserted data into table: {}", schemaTableName);
        } catch (Exception ex) {
            LOG.error("Unexpected error during insert operation for table: {}", schemaTableName, ex);
            throw new SQLException("Unexpected error during insert operation.", ex);
        }
    }
}
