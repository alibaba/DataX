package com.dorisdb.connector.datax.plugin.writer.doriswriter.row;

import java.io.Serializable;

import com.alibaba.datax.common.element.Record;

public interface DorisISerializer extends Serializable {

    String serialize(Record row);
    
}
