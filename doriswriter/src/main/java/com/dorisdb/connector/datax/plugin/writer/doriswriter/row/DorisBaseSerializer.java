package com.dorisdb.connector.datax.plugin.writer.doriswriter.row;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Column.Type;

public class DorisBaseSerializer {

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
