package com.starrocks.connector.datax.plugin.writer.starrockswriter.row;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Column.Type;

public class StarRocksBaseSerializer {

    protected String fieldConvertion(Column col) {
        if (null == col.getRawData()) {
            return null;
        }
        if (Type.BOOL == col.getType()) {
            return String.valueOf(col.asLong());
        }
        return col.asString();
    }

}
