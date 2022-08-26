package com.starrocks.connector.datax.plugin.writer.starrockswriter.row;

import java.io.Serializable;

import com.alibaba.datax.common.element.Record;

public interface StarRocksISerializer extends Serializable {

    String serialize(Record row);
    
}
