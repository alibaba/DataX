package com.alibaba.datax.plugin.reader.oceanbasev10reader.util;

/**
 * @author johnrobbet
 */

public enum PartType {
    NONPARTITION("NONPARTITION"),
    PARTITION("PARTITION"),
    SUBPARTITION("SUBPARTITION");

    private String typeString;

    PartType (String typeString) {
        this.typeString = typeString;
    }

    public String getTypeString() {
        return typeString;
    }
}


