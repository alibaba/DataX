package com.alibaba.datax.plugin.writer.conn;

import com.alibaba.datax.common.plugin.RecordSender;

import java.util.List;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：Connection for TSDB-like databases
 *
 * @author Benedict Jin
 * @since 2019-03-29
 */
public interface Connection4TSDB {

    /**
     * Get the address of Database.
     *
     * @return host+ip
     */
    String address();

    /**
     * Get the setted database name.
     *
     * @return database
     */
    String database();


    /**
     * Get the username of Database.
     *
     * @return username
     */
    String username();

    /**
     * Get the password of Database.
     *
     * @return password
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
     * Send data points by metric & start time & end time.
     *
     * @param metric       metric
     * @param start        startTime
     * @param end          endTime
     * @param recordSender sender
     */
    void sendDPs(String metric, Long start, Long end, RecordSender recordSender) throws Exception;

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
     * Put data points with single field.
     *
     * @param dps data points
     * @return whether the data point is written successfully
     */
    boolean put(String dps);

    /**
     * Put data points with multi fields.
     *
     * @param dps data points
     * @return whether the data point is written successfully
     */
    boolean mput(String dps);

    /**
     * Whether current version is supported.
     *
     * @return true: supported; false: not yet!
     */
    boolean isSupported();
}