package com.alibaba.datax.plugin.writer.otswriter.model;

import com.alicloud.openservices.tablestore.model.ColumnType;


public class OTSAttrColumn {
    // 该字段只在多版本中使用，表示多版本中，输入源中columnName的值，由将对应的Cell写入用户配置name的列中
    private String srcName = null;
    private String name = null;
    private ColumnType type = null;
    //该字段只在写入时序表时使用，该字段是否为时序数据的标签内部字段
    private  Boolean isTag = false;
    
    public OTSAttrColumn(String name, ColumnType type) {
        this.name = name;
        this.type = type;
    }
    
    public OTSAttrColumn(String srcName, String name, ColumnType type) {
        this.srcName = srcName;
        this.name = name;
        this.type = type;
    }

    public OTSAttrColumn(String name, ColumnType type, Boolean isTag) {
        this.name = name;
        this.type = type;
        this.isTag = isTag;
    }
    
    public String getName() {
        return name;
    }
    
    public ColumnType getType() {
        return type;
    }
    
    public String getSrcName() {
        return srcName;
    }

    public Boolean getTag() {
        return isTag;
    }
}
