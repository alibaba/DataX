package com.alibaba.datax.common.statistics;

import com.alibaba.datax.common.util.HostUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Created by liqiang on 15/8/23.
 */
@SuppressWarnings("NullableProblems")
public class PerfRecord implements Comparable<PerfRecord> {
    private static Logger perf = LoggerFactory.getLogger(PerfRecord.class);
    private static String datetimeFormat = "yyyy-MM-dd HH:mm:ss";


    public enum PHASE {
        /**
         * task total运行的时间，前10为框架统计，后面为部分插件的个性统计
         */
        TASK_TOTAL(0),

        READ_TASK_INIT(1),
        READ_TASK_PREPARE(2),
        READ_TASK_DATA(3),
        READ_TASK_POST(4),
        READ_TASK_DESTROY(5),

        WRITE_TASK_INIT(6),
        WRITE_TASK_PREPARE(7),
        WRITE_TASK_DATA(8),
        WRITE_TASK_POST(9),
        WRITE_TASK_DESTROY(10),

        /**
         * SQL_QUERY: sql query阶段, 部分reader的个性统计
         */
        SQL_QUERY(100),
        /**
         * 数据从sql全部读出来
         */
        RESULT_NEXT_ALL(101),

        /**
         * only odps block close
         */
        ODPS_BLOCK_CLOSE(102),

        WAIT_READ_TIME(103),

        WAIT_WRITE_TIME(104),

        TRANSFORMER_TIME(201);

        private int val;

        PHASE(int val) {
            this.val = val;
        }

        public int toInt(){
            return val;
        }
    }

    public enum ACTION{
        start,
        end
    }

    private final int taskGroupId;
    private final int taskId;
    private final PHASE phase;
    private volatile ACTION action;
    private volatile Date startTime;
    private volatile long elapsedTimeInNs = -1;
    private volatile long count = 0;
    private volatile long size = 0;

    private volatile long startTimeInNs;
    private volatile boolean isReport = false;

    public PerfRecord(int taskGroupId, int taskId, PHASE phase) {
        this.taskGroupId = taskGroupId;
        this.taskId = taskId;
        this.phase = phase;
    }

    public static void addPerfRecord(int taskGroupId, int taskId, PHASE phase, long startTime,long elapsedTimeInNs) {
        if(PerfTrace.getInstance().isEnable()) {
            PerfRecord perfRecord = new PerfRecord(taskGroupId, taskId, phase);
            perfRecord.elapsedTimeInNs = elapsedTimeInNs;
            perfRecord.action = ACTION.end;
            perfRecord.startTime = new Date(startTime);
            //在PerfTrace里注册
            PerfTrace.getInstance().tracePerfRecord(perfRecord);
            perf.info(perfRecord.toString());
        }
    }

    public void start() {
        if(PerfTrace.getInstance().isEnable()) {
            this.startTime = new Date();
            this.startTimeInNs = System.nanoTime();
            this.action = ACTION.start;
            //在PerfTrace里注册
            PerfTrace.getInstance().tracePerfRecord(this);
            perf.info(toString());
        }
    }

    public void addCount(long count) {
        this.count += count;
    }

    public void addSize(long size) {
        this.size += size;
    }

    public void end() {
        if(PerfTrace.getInstance().isEnable()) {
            this.elapsedTimeInNs = System.nanoTime() - startTimeInNs;
            this.action = ACTION.end;
            PerfTrace.getInstance().tracePerfRecord(this);
            perf.info(toString());
        }
    }

    public void end(long elapsedTimeInNs) {
        if(PerfTrace.getInstance().isEnable()) {
            this.elapsedTimeInNs = elapsedTimeInNs;
            this.action = ACTION.end;
            PerfTrace.getInstance().tracePerfRecord(this);
            perf.info(toString());
        }
    }

    public String toString() {
        return String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
                , getInstId(), taskGroupId, taskId, phase, action,
                DateFormatUtils.format(startTime, datetimeFormat), elapsedTimeInNs, count, size,getHostIP());
    }


    @Override
    public int compareTo(PerfRecord o) {
        if (o == null) {
            return 1;
        }
        return this.elapsedTimeInNs > o.elapsedTimeInNs ? 1 : this.elapsedTimeInNs == o.elapsedTimeInNs ? 0 : -1;
    }

    @Override
    public int hashCode() {
        long jobId = getInstId();
        int result = (int) (jobId ^ (jobId >>> 32));
        result = 31 * result + taskGroupId;
        result = 31 * result + taskId;
        result = 31 * result + phase.toInt();
        result = 31 * result + (startTime != null ? startTime.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if(!(o instanceof PerfRecord)){
            return false;
        }

        PerfRecord dst = (PerfRecord)o;

        if (this.getInstId() != dst.getInstId()) return false;
        if (this.taskGroupId != dst.taskGroupId) return false;
        if (this.taskId != dst.taskId) return false;
        if (phase != null ? !phase.equals(dst.phase) : dst.phase != null) return false;
        if (startTime != null ? !startTime.equals(dst.startTime) : dst.startTime != null) return false;
        return true;
    }

    public PerfRecord copy() {
        PerfRecord copy = new PerfRecord(this.taskGroupId, this.getTaskId(), this.phase);
        copy.action = this.action;
        copy.startTime = this.startTime;
        copy.elapsedTimeInNs = this.elapsedTimeInNs;
        copy.count = this.count;
        copy.size = this.size;
        return copy;
    }
    public int getTaskGroupId() {
        return taskGroupId;
    }

    public int getTaskId() {
        return taskId;
    }

    public PHASE getPhase() {
        return phase;
    }

    public ACTION getAction() {
        return action;
    }

    public long getElapsedTimeInNs() {
        return elapsedTimeInNs;
    }

    public long getCount() {
        return count;
    }

    public long getSize() {
        return size;
    }

    public long getInstId(){
        return PerfTrace.getInstance().getInstId();
    }

    public String getHostIP(){
       return HostUtils.IP;
    }

    public String getHostName(){
        return HostUtils.HOSTNAME;
    }

    public Date getStartTime() {
        return startTime;
    }

    public long getStartTimeInMs() {
        return startTime.getTime();
    }

    public long getStartTimeInNs() {
        return startTimeInNs;
    }

    public String getDatetime(){
        if(startTime == null){
            return "null time";
        }
        return DateFormatUtils.format(startTime, datetimeFormat);
    }

    public boolean isReport() {
        return isReport;
    }

    public void setIsReport(boolean isReport) {
        this.isReport = isReport;
    }
}
