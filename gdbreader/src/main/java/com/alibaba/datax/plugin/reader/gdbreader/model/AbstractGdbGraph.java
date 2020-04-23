/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */
package com.alibaba.datax.plugin.reader.gdbreader.model;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.gdbreader.Key;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author : Liu Jianping
 * @date : 2019/9/6
 */

public abstract class AbstractGdbGraph implements GdbGraph {
    final static int DEFAULT_TIMEOUT = 30000;
    private static final Logger log = LoggerFactory.getLogger(AbstractGdbGraph.class);
    private Client client;

    AbstractGdbGraph() {
    }

    AbstractGdbGraph(Configuration config) {
        log.info("init graphdb client");
        String host = config.getString(Key.HOST);
        int port = config.getInt(Key.PORT);
        String username = config.getString(Key.USERNAME);
        String password = config.getString(Key.PASSWORD);

        try {
            Cluster cluster = Cluster.build(host).port(port).credentials(username, password)
                    .serializer(Serializers.GRAPHBINARY_V1D0)
                    .maxContentLength(1024 * 1024)
                    .resultIterationBatchSize(64)
                    .create();
            client = cluster.connect().init();

            warmClient();
        } catch (RuntimeException e) {
            log.error("Failed to connect to GDB {}:{}, due to {}", host, port, e);
            throw e;
        }
    }

    protected List<Result> runInternal(String dsl, Map<String, Object> params) throws Exception {
        return runInternalAsync(dsl, params).all().get(DEFAULT_TIMEOUT + 1000, TimeUnit.MILLISECONDS);
    }

    protected ResultSet runInternalAsync(String dsl, Map<String, Object> params) throws Exception {
        RequestOptions.Builder options = RequestOptions.build().timeout(DEFAULT_TIMEOUT);
        if (params != null && !params.isEmpty()) {
            params.forEach(options::addParameter);
        }
        return client.submitAsync(dsl, options.create()).get(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    private void warmClient() {
        try {
            runInternal("g.V('test')", null);
            log.info("warm graphdb client over");
        } catch (Exception e) {
            log.error("warmClient error");
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            log.info("close graphdb client");
            client.close();
        }
    }
}
