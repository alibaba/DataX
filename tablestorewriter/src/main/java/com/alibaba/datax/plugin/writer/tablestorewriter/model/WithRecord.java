package com.alibaba.datax.plugin.writer.tablestorewriter.model;

import com.alibaba.datax.common.element.Record;

public interface WithRecord {
    Record getRecord();

    void setRecord(Record record);
}
