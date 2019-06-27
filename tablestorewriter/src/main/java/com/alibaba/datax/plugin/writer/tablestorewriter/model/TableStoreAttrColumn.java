package com.alibaba.datax.plugin.writer.tablestorewriter.model;

import com.alicloud.openservices.tablestore.model.ColumnType;

public class TableStoreAttrColumn implements Comparable<TableStoreAttrColumn> {
    private String name;
    private ColumnType type;
    private int sequence;

    public TableStoreAttrColumn() {
    }

    public TableStoreAttrColumn(String name, ColumnType type, int sequence) {
        this.name = name;
        this.type = type;
        this.sequence = sequence;
    }

    public String getName() {
        return name;
    }

    public ColumnType getType() {
        return type;
    }

    public int getSequence() {
        return sequence;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setType(ColumnType type) {
        this.type = type;
    }

    public void setSequence(int sequence) {
        this.sequence = sequence;
    }

    @Override
    public int compareTo(TableStoreAttrColumn o) {
        return getSequence() - o.getSequence();
    }
}
