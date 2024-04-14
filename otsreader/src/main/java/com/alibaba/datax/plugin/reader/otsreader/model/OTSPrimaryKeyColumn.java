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

    public com.alicloud.openservices.tablestore.model.PrimaryKeyType getType(Boolean newVersion) {
        com.alicloud.openservices.tablestore.model.PrimaryKeyType res = null;
        switch (this.type){
            case BINARY:
                res = com.alicloud.openservices.tablestore.model.PrimaryKeyType.BINARY;
                break;
            case INTEGER:
                res = com.alicloud.openservices.tablestore.model.PrimaryKeyType.INTEGER;
                break;
            case STRING:
            default:
                res = com.alicloud.openservices.tablestore.model.PrimaryKeyType.STRING;
                break;
        }
        return res;
    }

    public void setType(PrimaryKeyType type) {
        this.type = type;
    }

    public void setType(com.alicloud.openservices.tablestore.model.PrimaryKeyType type) {
        switch (type){
            case BINARY:
                this.type = PrimaryKeyType.BINARY;
                break;
            case INTEGER:
                this.type = PrimaryKeyType.INTEGER;
                break;
            case STRING:
            default:
                this.type = PrimaryKeyType.STRING;
                break;
        }
    }
    
}
