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

// A wrapper class to hold a batch of loaded rows
public class DorisFlushBatch {
    private String lineDelimiter;
    private String label;
    private long rows = 0;
    private StringBuilder data = new StringBuilder();

    public DorisFlushBatch(String lineDelimiter) {
        this.lineDelimiter = lineDelimiter;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getLabel() {
        return label;
    }

    public long getRows() {
        return rows;
    }

    public void putData(String row) {
        if (data.length() > 0) {
            data.append(lineDelimiter);
        }
        data.append(row);
        rows++;
    }

    public StringBuilder getData() {
        return data;
    }

    public long getSize() {
        return data.length();
    }
}
