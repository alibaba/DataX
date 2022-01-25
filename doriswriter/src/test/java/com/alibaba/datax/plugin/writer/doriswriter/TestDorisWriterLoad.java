// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.alibaba.datax.plugin.writer.doriswriter;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import com.alibaba.datax.common.util.Configuration;

import java.io.IOException;
import java.util.Map;

public class TestDorisWriterLoad {


    // for test
    public static void main(String[] args) throws IOException {
        /**
         * 下面示例使用的建表语句，要首先有一套Ddoris的环境，创建数据库demo，然后使用下面的建表语句创建表
         * 修改feLoadUrl中的IP地址，username，password，然后运行
         * CREATE TABLE `doris_test` (
         *   `k1` varchar(30) NULL ,
         *   `k2` varchar(255) NULL,
         *   `k3` varchar(200)
         * ) ENGINE=OLAP
         * Duplicate KEY(k1)
         * COMMENT "OLAP"
         * DISTRIBUTED BY HASH(k1) BUCKETS 1
         * PROPERTIES (
         * "replication_allocation" = "tag.location.default: 1",
         * "in_memory" = "false",
         * "storage_format" = "V2"
         * )
         */
        String json = "{\n" +
                "   \"feLoadUrl\": [\"127.0.0.1:8030\"],\n" +
                "   \"column\": [\"k1\", \"k2\", \"k3\"],\n" +
                "   \"database\": \"demo\",\n" +
                "   \"jdbcUrl\": \"\",\n" +
                "   \"loadProps\": {},\n" +
                "   \"password\": \"12345\",\n" +
                "   \"postSql\": [],\n" +
                "   \"preSql\": [],\n" +
                "   \"table\": \"doris_test\",\n" +
                "   \"username\": \"root\"\n" +
                "}";
        Configuration configuration = Configuration.from(json);
        Key key = new Key(configuration);

        DorisWriterEmitter emitter = new DorisWriterEmitter(key);
        DorisFlushBatch flushBatch = new DorisFlushBatch("\n");
        flushBatch.setLabel("test4");
        Map<String, String> row1 = Maps.newHashMap();
        row1.put("k1", "2021-02-02");
        row1.put("k2", "2021-02-02 00:00:00");
        row1.put("k3", "3");
        String rowStr1 = JSON.toJSONString(row1);
        System.out.println("rows1: " + rowStr1);
        flushBatch.putData(rowStr1);

        Map<String, String> row2 = Maps.newHashMap();
        row2.put("k1", "2021-02-03");
        row2.put("k2", "2021-02-03 00:00:00");
        row2.put("k3", "4");
        String rowStr2 = JSON.toJSONString(row2);
        System.out.println("rows2: " + rowStr2);
        flushBatch.putData(rowStr2);

        for (int i = 0; i < 50000; ++i) {
            flushBatch.putData(rowStr2);
        }
        emitter.doStreamLoad(flushBatch);
    }
}
