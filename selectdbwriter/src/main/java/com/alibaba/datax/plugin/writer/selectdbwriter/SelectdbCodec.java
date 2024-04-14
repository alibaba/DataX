package com.alibaba.datax.plugin.writer.selectdbwriter;

import com.alibaba.datax.common.element.Record;

import java.io.Serializable;

public interface SelectdbCodec extends Serializable {

    String codec( Record row);
}
