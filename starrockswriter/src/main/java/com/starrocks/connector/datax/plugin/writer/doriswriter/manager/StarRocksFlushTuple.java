package com.starrocks.connector.datax.plugin.writer.starrockswriter.manager;

import java.util.List;

public class StarRocksFlushTuple {
    
    private String label;
    private Long bytes;
    private List<String> rows;

    public StarRocksFlushTuple(String label, Long bytes, List<String> rows) {
        this.label = label;
        this.bytes = bytes;
        this.rows = rows;
    }

    public String getLabel() { return label; }
    public Long getBytes() { return bytes; }
    public List<String> getRows() { return rows; }
}