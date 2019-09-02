/**
 * 
 */
package com.alibaba.datax.plugin.writer.gdbwriter.model;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.gdbwriter.Key;
import com.alibaba.datax.plugin.writer.gdbwriter.client.GdbWriterConfig;

import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author jerrywang
 *
 */
@Slf4j
public abstract class AbstractGdbGraph implements GdbGraph {
	private final static int DEFAULT_TIMEOUT = 30000;

	protected Client client = null;
	protected Key.UpdateMode updateMode = Key.UpdateMode.INSERT;
	protected int propertiesBatchNum = GdbWriterConfig.DEFAULT_BATCH_PROPERTY_NUM;
	protected boolean session = false;


	protected AbstractGdbGraph() {}

	protected AbstractGdbGraph(Configuration config, boolean session) {
		initClient(config, session);
	}

	protected void initClient(Configuration config, boolean session) {
		updateMode = Key.UpdateMode.valueOf(config.getString(Key.UPDATE_MODE, "INSERT"));
		log.info("init graphdb client");
		String host = config.getString(Key.HOST);
		int port = config.getInt(Key.PORT);
		String username = config.getString(Key.USERNAME);
		String password = config.getString(Key.PASSWORD);
		int maxDepthPerConnection = config.getInt(Key.MAX_IN_PROCESS_PER_CONNECTION,
				GdbWriterConfig.DEFAULT_MAX_IN_PROCESS_PER_CONNECTION);

		int maxConnectionPoolSize = config.getInt(Key.MAX_CONNECTION_POOL_SIZE,
			GdbWriterConfig.DEFAULT_MAX_CONNECTION_POOL_SIZE);

		int maxSimultaneousUsagePerConnection = config.getInt(Key.MAX_SIMULTANEOUS_USAGE_PER_CONNECTION,
			GdbWriterConfig.DEFAULT_MAX_SIMULTANEOUS_USAGE_PER_CONNECTION);

		this.session = session;
		if (this.session) {
			maxConnectionPoolSize = GdbWriterConfig.DEFAULT_MAX_CONNECTION_POOL_SIZE;
			maxDepthPerConnection = GdbWriterConfig.DEFAULT_MAX_IN_PROCESS_PER_CONNECTION;
			maxSimultaneousUsagePerConnection = GdbWriterConfig.DEFAULT_MAX_SIMULTANEOUS_USAGE_PER_CONNECTION;
		}

		try {
			Cluster cluster = Cluster.build(host).port(port).credentials(username, password)
					.serializer(Serializers.GRAPHBINARY_V1D0)
					.maxContentLength(1048576)
				    .maxInProcessPerConnection(maxDepthPerConnection)
				    .minInProcessPerConnection(0)
				    .maxConnectionPoolSize(maxConnectionPoolSize)
				    .minConnectionPoolSize(maxConnectionPoolSize)
				    .maxSimultaneousUsagePerConnection(maxSimultaneousUsagePerConnection)
				    .resultIterationBatchSize(64)
				    .create();
			client = session ? cluster.connect(UUID.randomUUID().toString()).init() : cluster.connect().init();
			warmClient(maxConnectionPoolSize*maxDepthPerConnection);
		} catch (RuntimeException e) {
			log.error("Failed to connect to GDB {}:{}, due to {}", host, port, e);
			throw e;
		}

		propertiesBatchNum = config.getInt(Key.MAX_PROPERTIES_BATCH_NUM, GdbWriterConfig.DEFAULT_BATCH_PROPERTY_NUM);
	}


	/**
	 * @param dsl
	 * @param parameters
	 */
	protected void runInternal(String dsl, final Map<String, Object> parameters) throws Exception {
		RequestOptions.Builder options = RequestOptions.build().timeout(DEFAULT_TIMEOUT);
		if (parameters != null && !parameters.isEmpty()) {
			parameters.forEach(options::addParameter);
		}

		ResultSet results = client.submitAsync(dsl, options.create()).get(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
		results.all().get(DEFAULT_TIMEOUT + 1000, TimeUnit.MILLISECONDS);
	}

	void beginTx() {
		if (!session) {
			return;
		}

		String dsl = "g.tx().open()";
		client.submit(dsl).all().join();
	}

	void doCommit() {
		if (!session) {
			return;
		}

		try {
			String dsl = "g.tx().commit()";
			client.submit(dsl).all().join();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	void doRollback() {
		if (!session) {
			return;
		}

		String dsl = "g.tx().rollback()";
		client.submit(dsl).all().join();
	}

	private void warmClient(int num) {
		try {
			beginTx();
			runInternal("g.V('test')", null);
			doCommit();
			log.info("warm graphdb client over");
		} catch (Exception e) {
			log.error("warmClient error");
			doRollback();
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {
		if (client != null) {
			log.info("close graphdb client");
			client.close();
		}
	}
}
