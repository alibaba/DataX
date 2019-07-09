package com.alibaba.datax.plugin.reader.tablestorereader.model;

import com.alicloud.openservices.tablestore.model.PrimaryKeyType;

public class TableStorePrimaryKeyColumn {
    private String name;
    private PrimaryKeyType type;
    
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public PrimaryKeyType getType() {
        return type;
    }
    public void setType(PrimaryKeyType type) {
        this.type = type;
    }
    
}
