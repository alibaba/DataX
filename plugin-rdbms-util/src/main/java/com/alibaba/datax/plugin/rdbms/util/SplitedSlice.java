package com.alibaba.datax.plugin.rdbms.util;

public class SplitedSlice {
    private String begin;
    private String end;
    private String range;

    public SplitedSlice(String begin, String end, String range) {
        this.begin = begin;
        this.end = end;
        this.range = range;
    }

    public String getBegin() {
        return begin;
    }

    public void setBegin(String begin) {
        this.begin = begin;
    }

    public String getEnd() {
        return end;
    }

    public void setEnd(String end) {
        this.end = end;
    }

    public String getRange() {
        return range;
    }

    public void setRange(String range) {
        this.range = range;
    }
}
