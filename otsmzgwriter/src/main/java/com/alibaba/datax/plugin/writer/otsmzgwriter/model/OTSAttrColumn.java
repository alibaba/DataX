package com.alibaba.datax.plugin.writer.otsmzgwriter.model;

import com.alibaba.datax.plugin.writer.otsmzgwriter.utils.BeanCopierUtils;
import com.alibaba.fastjson.JSON;
import com.aliyun.openservices.ots.model.ColumnType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OTSAttrColumn implements Comparable<OTSAttrColumn> {
    private String name;
    private ColumnType type;
    private int sequence;

    public OTSAttrColumn() {
    }

    public OTSAttrColumn(String name, ColumnType type, int sequence) {
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
    public int compareTo(OTSAttrColumn o) {
        return getSequence() - o.getSequence();
    }
}
