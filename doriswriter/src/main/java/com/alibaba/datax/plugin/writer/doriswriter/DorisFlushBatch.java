package com.alibaba.datax.plugin.writer.doriswriter;

import java.util.List;

public class DorisFlushBatch
{
    private String label;
    private Long bytes;
    private List<String> rows;
    
    public DorisFlushBatch(final String label, final Long bytes, final List<String> rows) {
        this.label = label;
        this.bytes = bytes;
        this.rows = rows;
    }
    
    public String getLabel() {
        return this.label;
    }
    
    public Long getBytes() {
        return this.bytes;
    }
    
    public List<String> getRows() {
        return this.rows;
    }
}
