package com.alibaba.datax.plugin.reader.otsreader.model;

import com.aliyun.openservices.ots.model.PrimaryKeyType;

public class OTSPrimaryKeyColumn {
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
