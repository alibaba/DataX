package com.alibaba.datax.plugin.reader.conn;

import com.alibaba.datax.common.plugin.RecordSender;

import java.util.List;

//This file is part of OpenTSDB.

//Copyright (C) 2010-2012  The OpenTSDB Authors.
//Copyright（C）2019 Alibaba Group Holding Ltd.

//

//This program is free software: you can redistribute it and/or modify it

//under the terms of the GNU Lesser General Public License as published by

//the Free Software Foundation, either version 2.1 of the License, or (at your

//option) any later version.  This program is distributed in the hope that it

//will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty

//of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser

//General Public License for more details.  You should have received a copy

//of the GNU Lesser General Public License along with this program.  If not,

//see <http://www.gnu.org/licenses/>.
public interface Connection4TSDB {

    /**
     * Get the address of Database.
     *
     * @return host+ip
     */
    String address();

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
     * Whether current version is supported.
     *
     * @return true: supported; false: not yet!
     */
    boolean isSupported();
}
