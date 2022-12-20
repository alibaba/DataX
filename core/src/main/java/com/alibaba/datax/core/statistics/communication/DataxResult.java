package com.alibaba.datax.core.statistics.communication;

/**
 * @Author macross.zhang
 * @create 2022/12/19 15:19
 */
public class DataxResult {
    //任务启动时刻
    private long startTimeStamp;
    //任务结束时刻
    private long endTimeStamp;
    //任务总时耗
    private long totalCosts;
    //任务平均流量
    private long byteSpeedPerSecond;
    //记录写入速度
    private long recordSpeedPerSecond;
    //读出记录总数
    private long totalReadRecords;
    //读写失败总数
    private long totalErrorRecords;
    //成功记录总数
    private long transformerSucceedRecords;
    // 失败记录总数
    private long transformerFailedRecords;
    // 过滤记录总数
    private long transformerFilterRecords;
    //字节数
    private long readSucceedBytes;
    //转换开始时间
    private long endTransferTimeStamp;
    //转换结束时间
    private long startTransferTimeStamp;
    //转换总耗时
    private long transferCosts;

    public DataxResult(){}

    public DataxResult(long startTimeStamp, long endTimeStamp, long totalCosts, long byteSpeedPerSecond, long recordSpeedPerSecond, long totalReadRecords, long totalErrorRecords, long transformerSucceedRecords, long transformerFailedRecords, long transformerFilterRecords, long readSucceedBytes, long endTransferTimeStamp, long startTransferTimeStamp, long transferCosts) {
        this.startTimeStamp = startTimeStamp;
        this.endTimeStamp = endTimeStamp;
        this.totalCosts = totalCosts;
        this.byteSpeedPerSecond = byteSpeedPerSecond;
        this.recordSpeedPerSecond = recordSpeedPerSecond;
        this.totalReadRecords = totalReadRecords;
        this.totalErrorRecords = totalErrorRecords;
        this.transformerSucceedRecords = transformerSucceedRecords;
        this.transformerFailedRecords = transformerFailedRecords;
        this.transformerFilterRecords = transformerFilterRecords;
        this.readSucceedBytes = readSucceedBytes;
        this.endTransferTimeStamp = endTransferTimeStamp;
        this.startTransferTimeStamp = startTransferTimeStamp;
        this.transferCosts = transferCosts;
    }

    public long getStartTimeStamp() {
        return startTimeStamp;
    }

    public long getEndTimeStamp() {
        return endTimeStamp;
    }

    public long getTotalCosts() {
        return totalCosts;
    }

    public long getByteSpeedPerSecond() {
        return byteSpeedPerSecond;
    }

    public long getRecordSpeedPerSecond() {
        return recordSpeedPerSecond;
    }

    public long getTotalReadRecords() {
        return totalReadRecords;
    }

    public long getTotalErrorRecords() {
        return totalErrorRecords;
    }

    public long getTransformerSucceedRecords() {
        return transformerSucceedRecords;
    }

    public long getTransformerFailedRecords() {
        return transformerFailedRecords;
    }

    public long getTransformerFilterRecords() {
        return transformerFilterRecords;
    }

    public long getReadSucceedBytes() {
        return readSucceedBytes;
    }

    public long getEndTransferTimeStamp() {
        return endTransferTimeStamp;
    }

    public long getStartTransferTimeStamp() {
        return startTransferTimeStamp;
    }

    public long getTransferCosts() {
        return transferCosts;
    }

    public void setStartTimeStamp(long startTimeStamp) {
        this.startTimeStamp = startTimeStamp;
    }

    public void setEndTimeStamp(long endTimeStamp) {
        this.endTimeStamp = endTimeStamp;
    }

    public void setTotalCosts(long totalCosts) {
        this.totalCosts = totalCosts;
    }

    public void setByteSpeedPerSecond(long byteSpeedPerSecond) {
        this.byteSpeedPerSecond = byteSpeedPerSecond;
    }

    public void setRecordSpeedPerSecond(long recordSpeedPerSecond) {
        this.recordSpeedPerSecond = recordSpeedPerSecond;
    }

    public void setTotalReadRecords(long totalReadRecords) {
        this.totalReadRecords = totalReadRecords;
    }

    public void setTotalErrorRecords(long totalErrorRecords) {
        this.totalErrorRecords = totalErrorRecords;
    }

    public void setTransformerSucceedRecords(long transformerSucceedRecords) {
        this.transformerSucceedRecords = transformerSucceedRecords;
    }

    public void setTransformerFailedRecords(long transformerFailedRecords) {
        this.transformerFailedRecords = transformerFailedRecords;
    }

    public void setTransformerFilterRecords(long transformerFilterRecords) {
        this.transformerFilterRecords = transformerFilterRecords;
    }

    public void setReadSucceedBytes(long readSucceedBytes) {
        this.readSucceedBytes = readSucceedBytes;
    }

    public void setEndTransferTimeStamp(long endTransferTimeStamp) {
        this.endTransferTimeStamp = endTransferTimeStamp;
    }

    public void setStartTransferTimeStamp(long startTransferTimeStamp) {
        this.startTransferTimeStamp = startTransferTimeStamp;
    }

    public void setTransferCosts(long transferCosts) {
        this.transferCosts = transferCosts;
    }
}
