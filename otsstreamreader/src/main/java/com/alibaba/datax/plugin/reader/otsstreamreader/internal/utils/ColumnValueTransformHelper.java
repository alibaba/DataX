package com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.element.Column;
import com.alicloud.openservices.tablestore.model.*;

public class  ColumnValueTransformHelper {
    public static Column otsPrimaryKeyValueToDataxColumn(PrimaryKeyValue pkValue) {
        switch (pkValue.getType()) {
            case STRING:return new StringColumn(pkValue.asString());
            case INTEGER:return new LongColumn(pkValue.asLong());
            case BINARY:return new BytesColumn(pkValue.asBinary());
            default:
                throw new IllegalArgumentException("Unknown primary key type: " + pkValue.getType() + ".");
        }
    }

    public static Column otsColumnValueToDataxColumn(ColumnValue columnValue) {
        switch (columnValue.getType()) {
            case STRING:return new StringColumn(columnValue.asString());
            case INTEGER:return new LongColumn(columnValue.asLong());
            case BINARY:return new BytesColumn(columnValue.asBinary());
            case BOOLEAN:return new BoolColumn(columnValue.asBoolean());
            case DOUBLE:return new DoubleColumn(columnValue.asDouble());
            default:
                throw new IllegalArgumentException("Unknown column type: " + columnValue.getType() + ".");
        }
    }
}
