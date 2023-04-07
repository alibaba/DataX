package com.alibaba.datax.plugin.reader.oceanbasev10reader.util;

/**
 * @author johnrobbet
 */

public enum PartType {
    // Non partitioned table
    NONPARTITION("NONPARTITION"),

    // Partitioned table
    PARTITION("PARTITION"),

    // Subpartitioned table
    SUBPARTITION("SUBPARTITION");

    private String typeString;

    PartType (String typeString) {
        this.typeString = typeString;
    }

    public String getTypeString() {
        return typeString;
    }
}


