package com.alibaba.datax.plugin.writer.elasticsearchwriter;

public class PartitionColumn {
    private String name;
    // like: DATA
    private String metaType;
    private String comment;
    // like: VARCHAR
    private String type;

    public String getName() {
        return name;
    }

    public String getMetaType() {
        return metaType;
    }

    public String getComment() {
        return comment;
    }

    public String getType() {
        return type;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setMetaType(String metaType) {
        this.metaType = metaType;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setType(String type) {
        this.type = type;
    }
}