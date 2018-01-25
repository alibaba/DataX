package com.alibaba.datax.plugin.reader.otsreader.model;

import com.aliyun.openservices.ots.model.RowPrimaryKey;

public class OTSRange {
    
    private RowPrimaryKey begin = null;
    private RowPrimaryKey end = null;
    
    public OTSRange() {}
    
    public OTSRange(RowPrimaryKey begin, RowPrimaryKey end) {
        this.begin = begin;
        this.end = end;
    }
    
    public RowPrimaryKey getBegin() {
        return begin;
    }
    public void setBegin(RowPrimaryKey begin) {
        this.begin = begin;
    }
    public RowPrimaryKey getEnd() {
        return end;
    }
    public void setEnd(RowPrimaryKey end) {
        this.end = end;
    }
}
