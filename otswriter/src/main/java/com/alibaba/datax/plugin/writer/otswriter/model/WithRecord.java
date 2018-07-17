package com.alibaba.datax.plugin.writer.otswriter.model;

import com.alibaba.datax.common.element.Record;

public interface WithRecord {
    Record getRecord();

    void setRecord(Record record);
}
