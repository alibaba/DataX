package com.alibaba.datax.plugin.reader.otsreader.utils;

import com.alibaba.datax.common.element.*;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;

public class TranformHelper {
    
    public static Column otsPrimaryKeyColumnToDataxColumn(PrimaryKeyColumn pkc) {
        switch (pkc.getValue().getType()) {
            case STRING:return new StringColumn(pkc.getValue().asString());
            case INTEGER:return new LongColumn(pkc.getValue().asLong()); 
            case BINARY:return new BytesColumn(pkc.getValue().asBinary()); 
            default:
                throw new IllegalArgumentException("PrimaryKey unsuporrt tranform the type: " + pkc.getValue().getType() + ".");
        }
    }
    
    public static Column otsColumnToDataxColumn(com.alicloud.openservices.tablestore.model.Column c) {
        switch (c.getValue().getType()) {
            case STRING:return new StringColumn(c.getValue().asString());
            case INTEGER:return new LongColumn(c.getValue().asLong());
            case BINARY:return new BytesColumn(c.getValue().asBinary());
            case BOOLEAN:return new BoolColumn(c.getValue().asBoolean());
            case DOUBLE:return new DoubleColumn(c.getValue().asDouble());
            default:
                throw new IllegalArgumentException("Column unsuporrt tranform the type: " + c.getValue().getType() + ".");
            
        }
    }

    public static Column otsColumnToDataxColumn(com.alicloud.openservices.tablestore.model.ColumnValue c) {
        switch (c.getType()) {
            case STRING:return new StringColumn(c.asString());
            case INTEGER:return new LongColumn(c.asLong());
            case BINARY:return new BytesColumn(c.asBinary());
            case BOOLEAN:return new BoolColumn(c.asBoolean());
            case DOUBLE:return new DoubleColumn(c.asDouble());
            default:
                throw new IllegalArgumentException("Column unsuporrt tranform the type: " + c.getType() + ".");
        }
    }
}
