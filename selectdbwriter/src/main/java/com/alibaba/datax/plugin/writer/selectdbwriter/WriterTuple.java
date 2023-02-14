package com.alibaba.datax.plugin.writer.selectdbwriter;

import java.util.List;

public class WriterTuple {
    private String label;
    private Long bytes;
    private List<byte[]> rows;


    public WriterTuple ( String label, Long bytes, List<byte[]> rows){
        this.label = label;
        this.rows = rows;
        this.bytes = bytes;
    }

    public String getLabel() { return label; }
    public void setLabel(String label) { this.label = label; }
    public Long getBytes() { return bytes; }
    public List<byte[]> getRows() { return rows; }

}
