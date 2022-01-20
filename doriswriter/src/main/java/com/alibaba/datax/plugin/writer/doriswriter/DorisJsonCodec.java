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

import com.alibaba.datax.common.element.Record;
import com.alibaba.fastjson.JSON;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Convert DataX data to json
public class DorisJsonCodec extends DorisCodec {
    private Map<String, Object> rowMap;

    public DorisJsonCodec(final List<String> fieldNames) {
        super(fieldNames);
        this.rowMap = new HashMap<>(this.fieldNames.size());
    }

    @Override
    public String serialize(final Record row) {
        if (null == this.fieldNames) {
            return "";
        }

        rowMap.clear();
        int idx = 0;
        for (final String fieldName : this.fieldNames) {
            rowMap.put(fieldName, this.convertColumn(row.getColumn(idx)));
            ++idx;
        }
        return JSON.toJSONString(rowMap);
    }
}
