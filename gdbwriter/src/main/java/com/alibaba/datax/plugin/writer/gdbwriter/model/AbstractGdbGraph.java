/**
 * 
 */
package com.alibaba.datax.plugin.writer.gdbwriter.model;

import static com.alibaba.datax.plugin.writer.gdbwriter.client.GdbWriterConfig.DEFAULT_BATCH_PROPERTY_NUM;
import static com.alibaba.datax.plugin.writer.gdbwriter.client.GdbWriterConfig.MAX_REQUEST_LENGTH;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.gdbwriter.Key;
import com.alibaba.datax.plugin.writer.gdbwriter.client.GdbWriterConfig;

import lombok.extern.slf4j.Slf4j;

/**
 * @author jerrywang
 *
 */
@Slf4j
public abstract class AbstractGdbGraph implements GdbGraph {
    private final static int DEFAULT_TIMEOUT = 30000;

    protected Client client = null;
    protected Key.UpdateMode updateMode = Key.UpdateMode.INSERT;
    protected int propertiesBatchNum = DEFAULT_BATCH_PROPERTY_NUM;
    protected boolean session = false;
    protected int maxRequestLength = GdbWriterConfig.MAX_REQUEST_LENGTH;

    protected AbstractGdbGraph() {}

    protected AbstractGdbGraph(final Configuration config, final boolean session) {
        initClient(config, session);
    }

    protected void initClient(final Configuration config, final boolean session) {
        this.updateMode = Key.UpdateMode.valueOf(config.getString(Key.UPDATE_MODE, "INSERT"));
        log.info("init graphdb client");
        final String host = config.getString(Key.HOST);
        final int port = config.getInt(Key.PORT);
        final String username = config.getString(Key.USERNAME);
        final String password = config.getString(Key.PASSWORD);
        int maxDepthPerConnection =
            config.getInt(Key.MAX_IN_PROCESS_PER_CONNECTION, GdbWriterConfig.DEFAULT_MAX_IN_PROCESS_PER_CONNECTION);

        int maxConnectionPoolSize =
            config.getInt(Key.MAX_CONNECTION_POOL_SIZE, GdbWriterConfig.DEFAULT_MAX_CONNECTION_POOL_SIZE);

        int maxSimultaneousUsagePerConnection = config.getInt(Key.MAX_SIMULTANEOUS_USAGE_PER_CONNECTION,
            GdbWriterConfig.DEFAULT_MAX_SIMULTANEOUS_USAGE_PER_CONNECTION);

        this.session = session;
        if (this.session) {
            maxConnectionPoolSize = GdbWriterConfig.DEFAULT_MAX_CONNECTION_POOL_SIZE;
            maxDepthPerConnection = GdbWriterConfig.DEFAULT_MAX_IN_PROCESS_PER_CONNECTION;
            maxSimultaneousUsagePerConnection = GdbWriterConfig.DEFAULT_MAX_SIMULTANEOUS_USAGE_PER_CONNECTION;
        }

        try {
            final Cluster cluster = Cluster.build(host).port(port).credentials(username, password)
                .serializer(Serializers.GRAPHBINARY_V1D0).maxContentLength(1048576)
                .maxInProcessPerConnection(maxDepthPerConnection).minInProcessPerConnection(0)
                .maxConnectionPoolSize(maxConnectionPoolSize).minConnectionPoolSize(maxConnectionPoolSize)
                .maxSimultaneousUsagePerConnection(maxSimultaneousUsagePerConnection).resultIterationBatchSize(64)
                .create();
            this.client = session ? cluster.connect(UUID.randomUUID().toString()).init() : cluster.connect().init();
            warmClient(maxConnectionPoolSize * maxDepthPerConnection);
        } catch (final RuntimeException e) {
            log.error("Failed to connect to GDB {}:{}, due to {}", host, port, e);
            throw e;
        }

        this.propertiesBatchNum = config.getInt(Key.MAX_PROPERTIES_BATCH_NUM, DEFAULT_BATCH_PROPERTY_NUM);
        this.maxRequestLength = config.getInt(Key.MAX_GDB_REQUEST_LENGTH, MAX_REQUEST_LENGTH);
    }

    /**
     * @param dsl
     * @param parameters
     */
    protected void runInternal(final String dsl, final Map<String, Object> parameters) throws Exception {
        final RequestOptions.Builder options = RequestOptions.build().timeout(DEFAULT_TIMEOUT);
        if (parameters != null && !parameters.isEmpty()) {
            parameters.forEach(options::addParameter);
        }

        final ResultSet results = this.client.submitAsync(dsl, options.create()).get(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
        results.all().get(DEFAULT_TIMEOUT + 1000, TimeUnit.MILLISECONDS);
    }

    void beginTx() {
        if (!this.session) {
            return;
        }

        final String dsl = "g.tx().open()";
        this.client.submit(dsl).all().join();
    }

    void doCommit() {
        if (!this.session) {
            return;
        }

        try {
            final String dsl = "g.tx().commit()";
            this.client.submit(dsl).all().join();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    void doRollback() {
        if (!this.session) {
            return;
        }

        final String dsl = "g.tx().rollback()";
        this.client.submit(dsl).all().join();
    }

    private void warmClient(final int num) {
        try {
            beginTx();
            runInternal("g.V('test')", null);
            doCommit();
            log.info("warm graphdb client over");
        } catch (final Exception e) {
            log.error("warmClient error");
            doRollback();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (this.client != null) {
            log.info("close graphdb client");
            this.client.close();
        }
    }
}
