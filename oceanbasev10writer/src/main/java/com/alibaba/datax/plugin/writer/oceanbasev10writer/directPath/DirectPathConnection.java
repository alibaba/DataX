package com.alibaba.datax.plugin.writer.oceanbasev10writer.directPath;

import java.sql.SQLException;
import java.util.Arrays;

import com.alibaba.datax.common.util.Configuration;

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadBucket;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObLoadDupActionType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

public class DirectPathConnection extends AbstractRestrictedConnection {

    private static final int OB_DIRECT_PATH_DEFAULT_BLOCKS = 1;
    private static final long OB_DIRECT_PATH_HEART_BEAT_TIMEOUT = 60000;
    private static final long OB_DIRECT_PATH_HEART_BEAT_INTERVAL = 10000;
    private static final int DEFAULT_BUFFERSIZE = 1048576;
    private final Configuration configuration;

    private State state;
    private int commiters;

    private final int blocks;
    private final ObTableDirectLoad load;
    private final Object lock = new Object();

    private static final Logger log = LoggerFactory.getLogger(DirectPathConnection.class);

    /**
     * Construct a new instance.
     *
     * @param load
     * @param blocks
     */
    private DirectPathConnection(ObTableDirectLoad load, int blocks, Configuration configuration) {
        this.configuration = configuration;
        this.load = load;
        this.blocks = blocks;
    }

    /**
     * Begin a new {@link DirectPathConnection }
     *
     * @return DirectPathConnection
     * @throws SQLException
     */
    public DirectPathConnection begin() throws SQLException {
        synchronized (lock) {
            if (state == null || state == State.CLOSED) {
                try {
                    this.load.begin();
                    this.state = State.BEGIN;
                } catch (Exception ex) {
                    throw new SQLException(ex);
                }
            } else {
                throw new IllegalStateException("Begin transaction failed as connection state is already BEGIN");
            }
        }
        return this;
    }

    /**
     * Commit buffered data with MAXIMUM timeout.
     *
     * @throws SQLException
     */
    @Override
    public void commit() throws SQLException {
        synchronized (lock) {
            if (state == State.BEGIN) {
                this.commiters++;
                if (commiters == blocks) {
                    try {
                        this.load.commit();
                        state = State.FINISHED;
                    } catch (Exception ex) {
                        throw new SQLException(ex);
                    }
                } else if (commiters > blocks) {
                    throw new IllegalStateException("Your commit have exceed the limit. (" + commiters + ">" + blocks + ")");
                }
            } else {
                throw new IllegalStateException("Commit transaction failed as connection state is not BEGIN");
            }
        }
    }

    /**
     * Rollback if error occurred.
     *
     * @throws SQLException
     */
    @Override
    public void rollback() throws SQLException {
        synchronized (lock) {
            if (state == State.BEGIN) {
                try {
                    //obkv-table-client-2.1.0的close方法包含回滚逻辑
                    this.load.close();
                } catch (Exception ex) {
                    throw new SQLException(ex);
                }
            } else {
                throw new IllegalStateException("Rollback transaction failed as connection state is not BEGIN");
            }
        }
    }

    /**
     * Close this connection.
     */
    @Override
    public void close() {
        synchronized (lock) {
            // Closed only if state is BEGIN
            this.load.close();
            this.state = State.CLOSED;
        }
    }

    /**
     * @return DirectPathPreparedStatement
     */
    @Override
    public DirectPathPreparedStatement createStatement() throws SQLException {
        return this.prepareStatement(null);
    }

    /**
     * A new batch need create a new {@link DirectPathPreparedStatement }.
     * The {@link DirectPathPreparedStatement } can not be reuse, otherwise it may cause duplicate records.
     *
     * @return DirectPathStatement
     */
    @Override
    public DirectPathPreparedStatement prepareStatement(String sql) throws SQLException {
        if (state == State.BEGIN) {
            Integer bufferSize = configuration.getInt(DirectPathConstants.BUFFERSIZE, DEFAULT_BUFFERSIZE);
            log.info("The current bufferSize size is{}", bufferSize);
            return new DirectPathPreparedStatement(this, bufferSize);
        } else {
            throw new IllegalStateException("Create statement failed as connection state is not BEGIN");
        }
    }

    /**
     * Return the schema name of this connection instance.
     *
     * @return String
     */
    @Override
    public String getSchema() {
        if (state == State.BEGIN) {
            return this.load.getTable().getDatabase();
        } else {
            throw new IllegalStateException("Get schema failed as connection state is not BEGIN");
        }
    }

    /**
     * Return the table name of this connection instance.
     *
     * @return String
     */
    public String getTableName() {
        if (state == State.BEGIN) {
            return this.load.getTableName();
        } else {
            throw new IllegalStateException("Get table failed as connection state is not BEGIN");
        }
    }

    /**
     * Return whether this connection is closed.
     *
     * @return boolean
     */
    @Override
    public boolean isClosed() {
        synchronized (lock) {
            return this.state == State.CLOSED;
        }
    }

    public boolean isFinished() {
        return this.state.equals(State.FINISHED);
    }

    /**
     * Insert bucket data into buffer.
     *
     * @param bucket
     * @return int[]
     * @throws SQLException
     */
    int[] insert(ObDirectLoadBucket bucket) throws SQLException {
        try {
            this.load.write(bucket);
            int[] result = new int[bucket.getRowNum()];
            Arrays.fill(result, 1);
            return result;
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
    }

    /**
     * Indicates the state of {@link DirectPathConnection }
     */
    enum State {

        /**
         * Begin transaction
         */
        BEGIN,
        /**
         * Transaction is finished, ready to close.
         */
        FINISHED,

        /**
         * Transaction is closed.
         */
        CLOSED;
    }

    /**
     * This builder used to build a new {@link DirectPathConnection }
     */
    public static class Builder {

        private String host;
        private int port;

        private String user;
        private String tenant;
        private String password;

        private String schema;
        private String table;

        /**
         * Client job count.
         */
        private int blocks = OB_DIRECT_PATH_DEFAULT_BLOCKS;

        /**
         * Server threads used to sort.
         */
        private int parallel;

        private long maxErrorCount;

        private ObLoadDupActionType duplicateKeyAction;

        // Used for load data
        private long serverTimeout;

        private Configuration configuration;

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder user(String user) {
            this.user = user;
            return this;
        }

        public Builder tenant(String tenant) {
            this.tenant = tenant;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder schema(String schema) {
            this.schema = schema;
            return this;
        }

        public Builder table(String table) {
            this.table = table;
            return this;
        }

        public Builder blocks(int blocks) {
            this.blocks = blocks;
            return this;
        }

        public Builder parallel(int parallel) {
            this.parallel = parallel;
            return this;
        }

        public Builder maxErrorCount(long maxErrorCount) {
            this.maxErrorCount = maxErrorCount;
            return this;
        }

        public Builder duplicateKeyAction(ObLoadDupActionType duplicateKeyAction) {
            this.duplicateKeyAction = duplicateKeyAction;
            return this;
        }

        public Builder serverTimeout(long serverTimeout) {
            this.serverTimeout = serverTimeout;
            return this;
        }

        public Builder configuration(Configuration configuration) {
            this.configuration = configuration;
            return this;
        }

        /**
         * Build a new {@link DirectPathConnection }
         *
         * @return DirectPathConnection
         */
        public DirectPathConnection build() throws Exception {
            return createConnection(host, port, user, tenant, password, schema, table, //
                    blocks, parallel, maxErrorCount, duplicateKeyAction, serverTimeout, duplicateKeyAction).begin();
        }

        /**
         * Create a new {@link DirectPathConnection }
         *
         * @param host
         * @param port
         * @param user
         * @param tenant
         * @param password
         * @param schema
         * @param table
         * @param parallel
         * @param maxErrorCount
         * @param action
         * @param serverTimeout
         * @return DirectPathConnection
         * @throws Exception
         */
        DirectPathConnection createConnection(String host, int port, String user, String tenant, String password, String schema, String table, //
                int blocks, int parallel, long maxErrorCount, ObLoadDupActionType action, long serverTimeout, ObLoadDupActionType obLoadDupActionType) throws Exception {

            checkArgument(StringUtils.isNotBlank(host), "Host is null.(host=%s)", host);
            checkArgument((port > 0 && port < 65535), "Port is invalid.(port=%s)", port);
            checkArgument(StringUtils.isNotBlank(user), "User Name is null.(user=%s)", user);
            checkArgument(StringUtils.isNotBlank(tenant), "Tenant Name is null.(tenant=%s)", tenant);
            checkArgument(StringUtils.isNotBlank(schema), "Schema Name is null.(schema=%s)", schema);
            checkArgument(StringUtils.isNotBlank(table), "Table Name is null.(table=%s)", table);

            checkArgument(blocks > 0, "Client Blocks is invalid.(blocks=%s)", blocks);
            checkArgument(parallel > 0, "Server Parallel is invalid.(parallel=%s)", parallel);
            checkArgument(maxErrorCount > -1, "MaxErrorCount is invalid.(maxErrorCount=%s)", maxErrorCount);
            checkArgument(action != null, "ObLoadDupActionType is null.(obLoadDupActionType=%s)", action);
            checkArgument(serverTimeout > 0, "Server timeout is invalid.(timeout=%s)", serverTimeout);
            Long heartBeatTimeout = 0L;
            Long heartBeatInterval = 0L;
            if (configuration != null) {
                heartBeatTimeout = configuration.getLong(DirectPathConstants.HEART_BEAT_TIMEOUT, OB_DIRECT_PATH_HEART_BEAT_TIMEOUT);
                heartBeatInterval = configuration.getLong(DirectPathConstants.HEART_BEAT_INTERVAL, OB_DIRECT_PATH_HEART_BEAT_INTERVAL);
                parallel = configuration.getInt(DirectPathConstants.PARALLEL, parallel);
            }
            DirectLoaderBuilder builder = new DirectLoaderBuilder()
                    .host(host).port(port)
                    .user(user)
                    .tenant(tenant)
                    .password(password)
                    .schema(schema)
                    .table(table)
                    .parallel(parallel)
                    .maxErrorCount(maxErrorCount)
                    .timeout(serverTimeout)
                    .duplicateKeyAction(obLoadDupActionType)
                    .heartBeatTimeout(heartBeatTimeout)
                    .heartBeatInterval(heartBeatInterval);
            ObTableDirectLoad directLoad = builder.build();

            return new DirectPathConnection(directLoad, blocks, configuration);
        }
    }
}
