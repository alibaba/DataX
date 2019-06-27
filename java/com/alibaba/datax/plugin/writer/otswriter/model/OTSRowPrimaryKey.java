package com.alibaba.datax.plugin.writer.otswriter.model;

import java.util.Map;
import java.util.Map.Entry;

import com.aliyun.openservices.ots.model.PrimaryKeyValue;

public class OTSRowPrimaryKey {
    
    private Map<String, PrimaryKeyValue> columns;
    
    public OTSRowPrimaryKey(Map<String, PrimaryKeyValue> columns) {
        if (null == columns) {
            throw new IllegalArgumentException("Input columns can not be null.");
        }
        this.columns = columns;
    }
    
    public Map<String, PrimaryKeyValue> getColumns() {
        return columns;
    }

    @Override
    public int hashCode() {
        int result = 31;
        for (Entry<String, PrimaryKeyValue> entry : columns.entrySet()) {
            result = result ^ entry.getKey().hashCode() ^ entry.getValue().hashCode();
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof OTSRowPrimaryKey)) {
            return false;
        }
        OTSRowPrimaryKey other = (OTSRowPrimaryKey) obj;
        
        if (columns.size() != other.columns.size()) {
            return false;
        }
        
        for (Entry<String, PrimaryKeyValue> entry : columns.entrySet()) {
            PrimaryKeyValue otherValue = other.columns.get(entry.getKey());
            
            if (otherValue == null) {
                return false;
            }
            if (!otherValue.equals(entry.getValue())) {
                return false;
            }
        }
        return true;
    }
}
