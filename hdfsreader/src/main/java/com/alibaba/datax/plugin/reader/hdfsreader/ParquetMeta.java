package com.alibaba.datax.plugin.reader.hdfsreader;

import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;

/**
 * @author jitongchen
 * @date 2023/9/7 10:20 AM
 */
public class ParquetMeta {
    private String name;
    private OriginalType originalType;
    private PrimitiveType primitiveType;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public OriginalType getOriginalType() {
        return originalType;
    }

    public void setOriginalType(OriginalType originalType) {
        this.originalType = originalType;
    }

    public PrimitiveType getPrimitiveType() {
        return primitiveType;
    }

    public void setPrimitiveType(PrimitiveType primitiveType) {
        this.primitiveType = primitiveType;
    }
}