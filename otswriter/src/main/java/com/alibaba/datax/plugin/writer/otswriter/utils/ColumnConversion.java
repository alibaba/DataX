package com.alibaba.datax.plugin.writer.otswriter.utils;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.writer.otswriter.OTSCriticalException;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSAttrColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSErrorMessage;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;


/**
 * 备注：datax提供的转换机制有如下限制,如下规则是不能转换的
 * 1. bool   -> binary
 * 2. binary -> long, double, bool
 * 3. double -> bool, binary
 * 4. long   -> binary
 */
public class ColumnConversion {
    public static PrimaryKeyValue columnToPrimaryKeyValue(Column c, PrimaryKeySchema col) throws OTSCriticalException {
        try {
            switch (col.getType()) {
                case STRING:
                    return PrimaryKeyValue.fromString(c.asString());
                case INTEGER:
                    return PrimaryKeyValue.fromLong(c.asLong());
                case BINARY:
                    return PrimaryKeyValue.fromBinary(c.asBytes());
                default:
                    throw new OTSCriticalException(String.format(OTSErrorMessage.UNSUPPORT_PARSE, col.getType(), "PrimaryKeyValue"));
            }
        } catch (DataXException e) {
            throw new IllegalArgumentException(String.format(
                    OTSErrorMessage.COLUMN_CONVERSION_ERROR, 
                    c.getType(), c.asString(), col.getType().toString()
                    ),
                    e);
        }
    }
    
    public static ColumnValue columnToColumnValue(Column c) throws OTSCriticalException {
        switch (c.getType()) {
            case STRING:
                return ColumnValue.fromString(c.asString());
            case LONG:
                return ColumnValue.fromLong(c.asLong());
            case BOOL:
                return ColumnValue.fromBoolean(c.asBoolean());
            case DOUBLE:
                return ColumnValue.fromDouble(c.asDouble());
            case BYTES:
                return ColumnValue.fromBinary(c.asBytes());
            default:
                throw new OTSCriticalException(String.format(OTSErrorMessage.UNSUPPORT_PARSE, c.getType(), "ColumnValue"));
        }
    }
    
    public static ColumnValue columnToColumnValue(Column c, OTSAttrColumn col) throws OTSCriticalException {
        try {
            switch (col.getType()) {
                case STRING:
                    return ColumnValue.fromString(c.asString());
                case INTEGER:
                    return ColumnValue.fromLong(c.asLong());
                case BOOLEAN:
                    return ColumnValue.fromBoolean(c.asBoolean());
                case DOUBLE:
                    return ColumnValue.fromDouble(c.asDouble());
                case BINARY:
                    return ColumnValue.fromBinary(c.asBytes());
                default:
                    throw new OTSCriticalException(String.format(OTSErrorMessage.UNSUPPORT_PARSE, col.getType(), "ColumnValue"));
            }
        } catch (DataXException e) {
            throw new IllegalArgumentException(String.format(
                    OTSErrorMessage.COLUMN_CONVERSION_ERROR, 
                    c.getType(), c.asString(), col.getType().toString()
                    ),
                    e);
        }
    }
}
