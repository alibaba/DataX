package com.alibaba.datax.plugin.writer.tablestorewriter.model;

import com.alibaba.datax.common.element.Record;
import com.alicloud.openservices.tablestore.model.RowPutChange;

public class RowPutChangeWithRecord extends RowPutChange implements WithRecord {

    private Record record;

    public RowPutChangeWithRecord(String tableName) {
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
