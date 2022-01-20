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

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.Record;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.List;
import java.util.TimeZone;

public abstract class DorisCodec {
    protected static String timeZone = "GMT+8";
    protected static TimeZone timeZoner = TimeZone.getTimeZone(timeZone);
    protected final List<String> fieldNames;

    public DorisCodec(final List<String> fieldNames) {
        this.fieldNames = fieldNames;
    }

    public abstract String serialize(Record row);

    /**
     * convert datax internal  data to string
     *
     * @param col
     * @return
     */
    protected Object convertColumn(final Column col) {
        if (null == col.getRawData()) {
            return null;
        }
        Column.Type type = col.getType();
        switch (type) {
            case BOOL:
            case INT:
            case LONG:
                return col.asLong();
            case DOUBLE:
                return col.asDouble();
            case STRING:
                return col.asString();
            case DATE: {
                final DateColumn.DateType dateType = ((DateColumn) col).getSubType();
                switch (dateType) {
                    case DATE:
                        return DateFormatUtils.format(col.asDate(), "yyyy-MM-dd", timeZoner);
                    case DATETIME:
                        return DateFormatUtils.format(col.asDate(), "yyyy-MM-dd HH:mm:ss", timeZoner);
                    default:
                        return col.asString();
                }
            }
            default:
                // BAD, NULL, BYTES
                return null;
        }
    }
}
