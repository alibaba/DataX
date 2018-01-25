package com.alibaba.datax.plugin.writer.otswriter.model;

import com.aliyun.openservices.ots.model.ColumnType;

public class OTSAttrColumn {
    private String name;
    private ColumnType type;
    
    public OTSAttrColumn(String name, ColumnType type) {
        this.name = name;
        this.type = type;
    }
    
    public String getName() {
        return name;
    }
    
    public ColumnType getType() {
        return type;
    }
}
