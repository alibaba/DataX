package com.alibaba.datax.plugin.writer.mock;


import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.fastjson2.JSON;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockRecord implements Record {
    private static final int RECORD_AVERGAE_COLUMN_NUMBER = 16;

    private List<Column> columns;

    private int byteSize;


    private Map<String, Object> meta;

    public MockRecord() {
        this.columns = new ArrayList<>(RECORD_AVERGAE_COLUMN_NUMBER);
    }

    @Override
    public void addColumn(Column column) {
        columns.add(column);
        incrByteSize(column);
    }

    @Override
    public Column getColumn(int i) {
        if (i < 0 || i >= columns.size()) {
            return null;
        }
        return columns.get(i);
    }

    @Override
    public void setColumn(int i, final Column column) {
        if (i < 0) {
            throw new IllegalArgumentException("不能给index小于0的column设置值");
        }

        if (i >= columns.size()) {
            expandCapacity(i + 1);
        }

        decrByteSize(getColumn(i));
        this.columns.set(i, column);
        incrByteSize(getColumn(i));
    }

    @Override
    public String toString() {
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("size", this.getColumnNumber());
        json.put("data", this.columns);
        return JSON.toJSONString(json);
    }

    @Override
    public int getColumnNumber() {
        return this.columns.size();
    }

    @Override
    public int getByteSize() {
        return byteSize;
    }

    public int getMemorySize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setMeta(Map<String, String> meta) {

    }

    @Override
    public Map<String, String> getMeta() {
        return null;
    }

    private void decrByteSize(final Column column) {
    }

    private void incrByteSize(final Column column) {
    }

    private void expandCapacity(int totalSize) {
        if (totalSize <= 0) {
            return;
        }

        int needToExpand = totalSize - columns.size();
        while (needToExpand-- > 0) {
            this.columns.add(null);
        }
    }
}
