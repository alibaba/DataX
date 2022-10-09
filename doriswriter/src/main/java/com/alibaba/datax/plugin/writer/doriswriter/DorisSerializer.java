package com.alibaba.datax.plugin.writer.doriswriter;

import com.alibaba.datax.common.element.Record;

import java.io.Serializable;

public interface DorisSerializer extends Serializable {

    String serialize( Record row);
}
