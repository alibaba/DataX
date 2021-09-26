package com.alibaba.datax.plugin.reader.mongodbreader.util;


public class Range {
    private String lowerBound;
    private String upperBound;
    private int skip;
    private int limit;
    private boolean sampleType=true;

    public String getLowerBound() {
        return lowerBound;
    }

    public void setLowerBound(String lowerBound) {
        this.lowerBound = lowerBound;
    }

    public String getUpperBound() {
        return upperBound;
    }

    public void setUpperBound(String upperBound) {
        this.upperBound = upperBound;
    }

    public int getSkip() {
        return skip;
    }

    public void setSkip(int skip) {
        this.skip = skip;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public boolean isSampleType() {
        return sampleType;
    }

    public void setSampleType(boolean sampleType) {
        this.sampleType = sampleType;
    }
}