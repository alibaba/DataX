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

import java.util.ArrayList;
import java.util.List;

// A wrapper class to hold a batch of loaded rows
public class DorisFlushBatch {
    private final String format;
    private final String lineDelimiter;
    private String label;
    private long byteSize = 0;
    private List<String> data = new ArrayList<>();

    public DorisFlushBatch(String lineDelimiter, String format) {
        this.lineDelimiter = EscapeHandler.escapeString(lineDelimiter);
        this.format = format;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getLabel() {
        return label;
    }

    public long getRows() {
        return data.size();
    }

    public void putData(String row) {
        data.add(row);
        byteSize += row.getBytes().length;
    }

    public String getData() {
        String result;
        if (Key.DEFAULT_FORMAT_CSV.equalsIgnoreCase(format)) {
            result = String.join(this.lineDelimiter, data);
        } else {
            result = data.toString();
        }
        return result;
    }

    public long getSize() {
        return byteSize;
    }
}
