package com.alibaba.datax.plugin.reader.tsdbreader.conn;

import com.alibaba.datax.common.plugin.RecordSender;

import java.util.List;
import java.util.Map;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：Connection for TSDB-like databases
 *
 * @author Benedict Jin
 * @since 2019-10-21
 */
public interface Connection4TSDB {

    /**
     * Get the address of Database.
     *
     * @return host+ip
     */
    String address();

    /**
     * Get the address of Database.
     *
     * @return host+ip
     */
    String username();

    /**
     * Get the address of Database.
     *
     * @return host+ip
     */
    String password();

    /**
     * Get the version of Database.
     *
     * @return version
     */
    String version();

    /**
     * Get these configurations.
     *
     * @return configs
     */
    String config();

    /**
     * Get the list of supported version.
     *
     * @return version list
     */
    String[] getSupportVersionPrefix();

    /**
     * Send data points for TSDB with single field.
     */
    void sendDPs(String metric, Map<String, String> tags, Long start, Long end, RecordSender recordSender, Map<String, Object> hint) throws Exception;

    /**
     * Send data points for TSDB with multi fields.
     */
    void sendDPs(String metric, List<String> fields, Map<String, String> tags, Long start, Long end, RecordSender recordSender, Map<String, Object> hint) throws Exception;

    /**
     * Send data points for RDB with single field.
     */
    void sendRecords(String metric, Map<String, String> tags, Long start, Long end, List<String> columns4RDB, Boolean isCombine, RecordSender recordSender, Map<String, Object> hint) throws Exception;

    /**
     * Send data points for RDB with multi fields.
     */
    void sendRecords(String metric, List<String> fields, Map<String, String> tags, Long start, Long end, List<String> columns4RDB, RecordSender recordSender, Map<String, Object> hint) throws Exception;

    /**
     * Send data points for RDB with single fields on combine mode.
     */
    void sendRecords(List<String> metrics, Map<String, String> tags, Long start, Long end, List<String> columns4RDB, RecordSender recordSender, Map<String, Object> hint) throws Exception;

    /**
     * Put data point.
     *
     * @param dp data point
     * @return whether the data point is written successfully
     */
    boolean put(DataPoint4TSDB dp);

    /**
     * Put data points.
     *
     * @param dps data points
     * @return whether the data point is written successfully
     */
    boolean put(List<DataPoint4TSDB> dps);

    /**
     * Whether current version is supported.
     *
     * @return true: supported; false: not yet!
     */
    boolean isSupported();
}
