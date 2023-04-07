package com.alibaba.datax.plugin.reader.util;

import com.alibaba.datax.plugin.reader.conn.DataPoint4TSDB;
import com.alibaba.fastjson2.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public final class TSDBUtils {

    private static final Logger LOG = LoggerFactory.getLogger(TSDBUtils.class);

    private TSDBUtils() {
    }

    public static String version(String address) {
        String url = String.format("%s/api/version", address);
        String rsp;
        try {
            rsp = HttpUtils.get(url);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return rsp;
    }

    public static String config(String address) {
        String url = String.format("%s/api/config", address);
        String rsp;
        try {
            rsp = HttpUtils.get(url);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return rsp;
    }

    public static boolean put(String address, List<DataPoint4TSDB> dps) {
        return put(address, JSON.toJSON(dps));
    }

    public static boolean put(String address, DataPoint4TSDB dp) {
        return put(address, JSON.toJSON(dp));
    }

    private static boolean put(String address, Object o) {
        String url = String.format("%s/api/put", address);
        String rsp;
        try {
            rsp = HttpUtils.post(url, o.toString());
            // If successful, the returned content should be null.
            assert rsp == null;
        } catch (Exception e) {
            LOG.error("Address: {}, DataPoints: {}", url, o);
            throw new RuntimeException(e);
        }
        return true;
    }
}
