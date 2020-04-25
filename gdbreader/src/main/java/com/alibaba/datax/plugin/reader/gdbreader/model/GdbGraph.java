/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */
package com.alibaba.datax.plugin.reader.gdbreader.model;

import org.apache.tinkerpop.gremlin.driver.ResultSet;

import java.util.List;
import java.util.Map;

/**
 * @author : Liu Jianping
 * @date : 2019/9/6
 */

public interface GdbGraph extends AutoCloseable {

    /**
     * Get All labels of GraphDB
     *
     * @return labels map included numbers
     */
    Map<String, Long> getLabels();

    /**
     * Get the Ids list of special 'label', size up to 'limit'
     *
     * @param label is Label of Vertex or Edge
     * @param start of Ids range to get
     * @param limit size of Ids list
     * @return Ids list
     */
    List<String> fetchIds(String label, String start, long limit);

    /**
     * Fetch element in async mode, just send query dsl to server
     *
     * @param label     node label to filter
     * @param start     range begin(included)
     * @param end       range end(included)
     * @param propNames propKey list to fetch
     * @return future to get result later
     */
    ResultSet fetchElementsAsync(String label, String start, String end, List<String> propNames);

    /**
     * Get get element from Response @{ResultSet}
     *
     * @param results Response of Server
     * @return element sets
     */
    List<GdbElement> getElement(ResultSet results);

    /**
     * close graph client
     *
     * @throws Exception if fails
     */
    @Override
    void close() throws Exception;
}
