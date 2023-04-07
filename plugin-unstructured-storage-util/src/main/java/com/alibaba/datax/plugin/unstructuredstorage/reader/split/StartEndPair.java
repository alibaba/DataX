package com.alibaba.datax.plugin.unstructuredstorage.reader.split;

/**
 * @Author: guxuan
 * @Date 2022-05-17 15:50
 */
public class StartEndPair {
    private Long start;
    private Long end;
    private String filePath;

    public StartEndPair() {
    }

    public StartEndPair(Long start, Long end, String filePath) {
        this.start = start;
        this.end = end;
        this.filePath = filePath;
    }

    public Long getEnd() {
        return end;
    }

    public void setEnd(Long end) {
        this.end = end;
    }

    public Long getStart() {
        return start;
    }

    public void setStart(Long start) {
        this.start = start;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public String toString() {
        return "StartEndPair [start=" + start + ", end=" + end + ", filePath=" + filePath + "]";
    }
}
