package com.alibaba.datax.plugin.writer.otswriter.utils;

import com.alibaba.datax.common.element.Record;

public interface WithRecord {
    Record getRecord();

    void setRecord(Record record);
}
