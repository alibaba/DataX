package com.alibaba.datax.plugin.writer.tablestorewriter.model;

import com.alicloud.openservices.tablestore.model.PrimaryKeyType;

public class TableStorePKColumn {
    private String name;
    private PrimaryKeyType type;
    
    public TableStorePKColumn(String name, PrimaryKeyType type) {
        this.name = name;
        this.type = type;
    }
    
    public PrimaryKeyType getType() {
        return type;
    }

    public String getName() {
        return name;
    }
    
}
