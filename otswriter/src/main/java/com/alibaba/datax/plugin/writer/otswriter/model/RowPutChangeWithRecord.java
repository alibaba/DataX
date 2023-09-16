package com.alibaba.datax.plugin.writer.otswriter.model;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.writer.otswriter.utils.WithRecord;

public class RowPutChangeWithRecord extends com.aliyun.openservices.ots.model.RowPutChange implements WithRecord {

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
