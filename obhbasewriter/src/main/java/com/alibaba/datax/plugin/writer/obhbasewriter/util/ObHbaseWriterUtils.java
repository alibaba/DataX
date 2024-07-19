/*
 * Copyright (c) 2021 OceanBase ob-loader-dumper is licensed under Mulan PSL v2. You can use this software according to
 * the terms and conditions of the Mulan PSL v2. You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the Mulan PSL v2 for more
 * details.
 */
package com.alibaba.datax.plugin.writer.obhbasewriter.util;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.MessageSource;
import com.alibaba.datax.plugin.writer.obhbasewriter.ColumnType;
import com.alibaba.datax.plugin.writer.obhbasewriter.Hbase094xWriterErrorCode;
import com.alibaba.datax.plugin.writer.obhbasewriter.ObHTableInfo;
import com.alibaba.datax.plugin.writer.obhbasewriter.task.PutTask;
import java.nio.charset.Charset;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author cjyyz
 * @date 2023/03/23
 * @since
 */
public class ObHbaseWriterUtils {

    private static final MessageSource MESSAGE_SOURCE = MessageSource.loadResourceBundle(PutTask.class);

    public static byte[] getRowkey(Record record, ObHTableInfo obHTableInfo) {
        byte[] rowkeyBuffer = {};
        for (Triple<Integer, String, ColumnType> rowKeyElement : obHTableInfo.getRowKeyElementList()) {
            Integer index = rowKeyElement.getLeft();
            ColumnType columnType = rowKeyElement.getRight();
            if (index == -1) {
                String value = rowKeyElement.getMiddle();
                rowkeyBuffer = Bytes.add(rowkeyBuffer, getValueByte(columnType, value, obHTableInfo.getEncoding()));
            } else {
                if (index >= record.getColumnNumber()) {
                    throw DataXException.asDataXException(Hbase094xWriterErrorCode.CONSTRUCT_ROWKEY_ERROR, MESSAGE_SOURCE.message("normaltask.3", record.getColumnNumber(), index));
                }
                byte[] value = getColumnByte(columnType, record.getColumn(index), obHTableInfo);
                rowkeyBuffer = Bytes.add(rowkeyBuffer, value);
            }
        }

        return rowkeyBuffer;
    }

    public static byte[] getColumnByte(ColumnType columnType, Column column, ObHTableInfo obHTableInfo) {
        byte[] bytes;
        if (column.getRawData() != null && !(columnType == ColumnType.STRING && column.asString().equals("null"))) {
            switch (columnType) {
                case INT:
                    bytes = Bytes.toBytes(column.asLong().intValue());
                    break;
                case LONG:
                    bytes = Bytes.toBytes(column.asLong());
                    break;
                case DOUBLE:
                    bytes = Bytes.toBytes(column.asDouble());
                    break;
                case FLOAT:
                    bytes = Bytes.toBytes(column.asDouble().floatValue());
                    break;
                case SHORT:
                    bytes = Bytes.toBytes(column.asLong().shortValue());
                    break;
                case BOOLEAN:
                    bytes = Bytes.toBytes(column.asBoolean());
                    break;
                case STRING:
                    bytes = getValueByte(columnType, column.asString(), obHTableInfo.getEncoding());
                    break;
                case BINARY:
                    bytes = Bytes.toBytesBinary(column.asString());
                    break;
                default:
                    throw DataXException.asDataXException(Hbase094xWriterErrorCode.ILLEGAL_VALUE, MESSAGE_SOURCE.message("hbaseabstracttask.2", columnType));
            }
        } else {
            switch (obHTableInfo.getNullModeType()) {
                case Skip:
                    bytes = null;
                    break;
                case Empty:
                    bytes = HConstants.EMPTY_BYTE_ARRAY;
                    break;
                default:
                    throw DataXException.asDataXException(Hbase094xWriterErrorCode.ILLEGAL_VALUE, MESSAGE_SOURCE.message("hbaseabstracttask.3"));
            }
        }
        return bytes;
    }

    /**
     * @param columnType
     * @param value
     * @return byte[]
     */
    private static byte[] getValueByte(ColumnType columnType, String value, String encoding) {
        byte[] bytes;
        if (value != null) {
            switch (columnType) {
                case INT:
                    bytes = Bytes.toBytes(Integer.parseInt(value));
                    break;
                case LONG:
                    bytes = Bytes.toBytes(Long.parseLong(value));
                    break;
                case DOUBLE:
                    bytes = Bytes.toBytes(Double.parseDouble(value));
                    break;
                case FLOAT:
                    bytes = Bytes.toBytes(Float.parseFloat(value));
                    break;
                case SHORT:
                    bytes = Bytes.toBytes(Short.parseShort(value));
                    break;
                case BOOLEAN:
                    bytes = Bytes.toBytes(Boolean.parseBoolean(value));
                    break;
                case STRING:
                    bytes = value.getBytes(Charset.forName(encoding));
                    break;
                default:
                    throw DataXException.asDataXException(Hbase094xWriterErrorCode.ILLEGAL_VALUE, MESSAGE_SOURCE.message("hbaseabstracttask.4", columnType));
            }
        } else {
            bytes = HConstants.EMPTY_BYTE_ARRAY;
        }
        return bytes;
    }
}