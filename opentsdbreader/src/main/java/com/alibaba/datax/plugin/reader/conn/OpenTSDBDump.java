package com.alibaba.datax.plugin.reader.conn;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.fastjson.JSON;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

import java.util.Map;

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
final class OpenTSDBDump {

    private static TSDB TSDB_INSTANCE;

    private OpenTSDBDump() {
    }

    static void dump(OpenTSDBConnection conn, String metric, Long start, Long end, RecordSender sender) throws Exception {
        DumpSeries.doDump(getTSDB(conn), new String[]{start + "", end + "", "none", metric}, sender);
    }

    private static TSDB getTSDB(OpenTSDBConnection conn) {
        if (TSDB_INSTANCE == null) {
            synchronized (TSDB.class) {
                if (TSDB_INSTANCE == null) {
                    try {
                        Config config = new Config(false);
                        Map configurations = JSON.parseObject(conn.config(), Map.class);
                        for (Object key : configurations.keySet()) {
                            config.overrideConfig(key.toString(), configurations.get(key.toString()).toString());
                        }
                        TSDB_INSTANCE = new TSDB(config);
                    } catch (Exception e) {
                        throw new RuntimeException("Cannot init OpenTSDB connection!");
                    }
                }
            }
        }
        return TSDB_INSTANCE;
    }
}
