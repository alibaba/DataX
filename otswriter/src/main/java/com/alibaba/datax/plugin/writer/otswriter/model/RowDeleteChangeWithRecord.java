package com.alibaba.datax.plugin.writer.otswriter.model;

import com.alibaba.datax.common.element.Record;

public class RowDeleteChangeWithRecord extends com.aliyun.openservices.ots.model.RowDeleteChange implements WithRecord {

    private Record record;

    public RowDeleteChangeWithRecord(String tableName) {
        super(tableName);
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public void setRecord(Record record) {
        this.record = record;
    }
}
