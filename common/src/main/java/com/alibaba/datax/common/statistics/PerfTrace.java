package com.alibaba.datax.common.statistics;

import com.alibaba.datax.common.statistics.PerfRecord.PHASE;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.HostUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * PerfTrace 记录 job（local模式），taskGroup（distribute模式），因为这2种都是jvm，即一个jvm里只需要有1个PerfTrace。
 */

public class PerfTrace {

    private static Logger LOG = LoggerFactory.getLogger(PerfTrace.class);
    private static PerfTrace instance;
    private static final Object lock = new Object();
    private String perfTraceId;
    private volatile boolean enable;
    private volatile boolean isJob;
    private long instId;
    private long jobId;
    private long jobVersion;
    private int taskGroupId;
    private int channelNumber;

    private int batchSize = 500;
    private volatile boolean perfReportEnable = true;

    //jobid_jobversion,instanceid,taskid, src_mark, dst_mark,
    private Map<Integer, String> taskDetails = new ConcurrentHashMap<Integer, String>();
    //PHASE => PerfRecord
    private ConcurrentHashMap<PHASE, SumPerfRecord4Print> perfRecordMaps4print = new ConcurrentHashMap<PHASE, SumPerfRecord4Print>();
    // job_phase => SumPerf4Report
    private SumPerf4Report sumPerf4Report = new SumPerf4Report();
    private SumPerf4Report sumPerf4Report4NotEnd;
    private Configuration jobInfo;
    private final Set<PerfRecord> needReportPool4NotEnd = new HashSet<PerfRecord>();
    private final List<PerfRecord> totalEndReport = new ArrayList<PerfRecord>();

    /**
     * 单实例
     *
     * @param isJob
     * @param jobId
     * @param taskGroupId
     * @return
     */
    public static PerfTrace getInstance(boolean isJob, long jobId, int taskGroupId, boolean enable) {

        if (instance == null) {
            synchronized (lock) {
                if (instance == null) {
                    instance = new PerfTrace(isJob, jobId, taskGroupId, enable);
                }
            }
        }
        return instance;
    }

    /**
     * 因为一个JVM只有一个，因此在getInstance(isJob,jobId,taskGroupId)调用完成实例化后，方便后续调用，直接返回该实例
     *
     * @return
     */
    public static PerfTrace getInstance() {
        if (instance == null) {
            LOG.error("PerfTrace instance not be init! must have some error! ");
            synchronized (lock) {
                if (instance == null) {
                    instance = new PerfTrace(false, -1111, -1111, false);
                }
            }
        }
        return instance;
    }

    private PerfTrace(boolean isJob, long jobId, int taskGroupId, boolean enable) {
        try {
            this.perfTraceId = isJob ? "job_" + jobId : String.format("taskGroup_%s_%s", jobId, taskGroupId);
            this.enable = enable;
            this.isJob = isJob;
            this.taskGroupId = taskGroupId;
            this.instId = jobId;
            LOG.info(String.format("PerfTrace traceId=%s, isEnable=%s", this.perfTraceId, this.enable));

        } catch (Exception e) {
            // do nothing
            this.enable = false;
        }
    }

    public void addTaskDetails(int taskId, String detail) {
        if (enable) {
            String before = "";
            int index = detail.indexOf("?");
            String current = detail.substring(0, index == -1 ? detail.length() : index);
            if (current.indexOf("[") >= 0) {
                current += "]";
            }
            if (taskDetails.containsKey(taskId)) {
                before = taskDetails.get(taskId).trim();
            }
            if (StringUtils.isEmpty(before)) {
                before = "";
            } else {
                before += ",";
            }
            this.taskDetails.put(taskId, before + current);
        }
    }

    public void tracePerfRecord(PerfRecord perfRecord) {
        try {
            if (enable) {
                long curNanoTime = System.nanoTime();
                //ArrayList非线程安全
                switch (perfRecord.getAction()) {
                    case end:
                        synchronized (totalEndReport) {
                            totalEndReport.add(perfRecord);

                            if (totalEndReport.size() > batchSize * 10) {
                                sumPerf4EndPrint(totalEndReport);
                            }
                        }

                        if (perfReportEnable && needReport(perfRecord)) {
                            synchronized (needReportPool4NotEnd) {
                                sumPerf4Report.add(curNanoTime,perfRecord);
                                needReportPool4NotEnd.remove(perfRecord);
                            }
                        }

                        break;
                    case start:
                        if (perfReportEnable && needReport(perfRecord)) {
                            synchronized (needReportPool4NotEnd) {
                                needReportPool4NotEnd.add(perfRecord);
                            }
                        }
                        break;
                }
            }
        } catch (Exception e) {
            // do nothing
        }
    }

    private boolean needReport(PerfRecord perfRecord) {
        switch (perfRecord.getPhase()) {
            case TASK_TOTAL:
            case SQL_QUERY:
            case RESULT_NEXT_ALL:
            case ODPS_BLOCK_CLOSE:
                return true;
        }
        return false;
    }

    public String summarizeNoException() {
        String res;
        try {
            res = summarize();
        } catch (Exception e) {
            res = "PerfTrace summarize has Exception " + e.getMessage();
        }
        return res;
    }

    //任务结束时，对当前的perf总汇总统计
    private synchronized String summarize() {
        if (!enable) {
            return "PerfTrace not enable!";
        }

        if (totalEndReport.size() > 0) {
            sumPerf4EndPrint(totalEndReport);
        }

        StringBuilder info = new StringBuilder();
        info.append("\n === total summarize info === \n");
        info.append("\n   1. all phase average time info and max time task info: \n\n");
        info.append(String.format("%-20s | %18s | %18s | %18s | %18s | %-100s\n", "PHASE", "AVERAGE USED TIME", "ALL TASK NUM", "MAX USED TIME", "MAX TASK ID", "MAX TASK INFO"));

        List<PHASE> keys = new ArrayList<PHASE>(perfRecordMaps4print.keySet());
        Collections.sort(keys, new Comparator<PHASE>() {
            @Override
            public int compare(PHASE o1, PHASE o2) {
                return o1.toInt() - o2.toInt();
            }
        });
        for (PHASE phase : keys) {
            SumPerfRecord4Print sumPerfRecord = perfRecordMaps4print.get(phase);
            if (sumPerfRecord == null) {
                continue;
            }
            long averageTime = sumPerfRecord.getAverageTime();
            long maxTime = sumPerfRecord.getMaxTime();
            int maxTaskId = sumPerfRecord.maxTaskId;
            int maxTaskGroupId = sumPerfRecord.getMaxTaskGroupId();
            info.append(String.format("%-20s | %18s | %18s | %18s | %18s | %-100s\n",
                    phase, unitTime(averageTime), sumPerfRecord.totalCount, unitTime(maxTime), jobId + "-" + maxTaskGroupId + "-" + maxTaskId, taskDetails.get(maxTaskId)));
        }

        //SumPerfRecord4Print countSumPerf = Optional.fromNullable(perfRecordMaps4print.get(PHASE.READ_TASK_DATA)).or(new SumPerfRecord4Print());

        SumPerfRecord4Print countSumPerf = perfRecordMaps4print.get(PHASE.READ_TASK_DATA);
        if(countSumPerf == null){
            countSumPerf = new SumPerfRecord4Print();
        }

        long averageRecords = countSumPerf.getAverageRecords();
        long averageBytes = countSumPerf.getAverageBytes();
        long maxRecord = countSumPerf.getMaxRecord();
        long maxByte = countSumPerf.getMaxByte();
        int maxTaskId4Records = countSumPerf.getMaxTaskId4Records();
        int maxTGID4Records = countSumPerf.getMaxTGID4Records();

        info.append("\n\n 2. record average count and max count task info :\n\n");
        info.append(String.format("%-20s | %18s | %18s | %18s | %18s | %18s | %-100s\n", "PHASE", "AVERAGE RECORDS", "AVERAGE BYTES", "MAX RECORDS", "MAX RECORD`S BYTES", "MAX TASK ID", "MAX TASK INFO"));
        if (maxTaskId4Records > -1) {
            info.append(String.format("%-20s | %18s | %18s | %18s | %18s | %18s | %-100s\n"
                    , PHASE.READ_TASK_DATA, averageRecords, unitSize(averageBytes), maxRecord, unitSize(maxByte), jobId + "-" + maxTGID4Records + "-" + maxTaskId4Records, taskDetails.get(maxTaskId4Records)));

        }
        return info.toString();
    }

    //缺省传入的时间是nano
    public static String unitTime(long time) {
        return unitTime(time, TimeUnit.NANOSECONDS);
    }

    public static String unitTime(long time, TimeUnit timeUnit) {
        return String.format("%,.3fs", ((float) timeUnit.toNanos(time)) / 1000000000);
    }

    public static String unitSize(long size) {
        if (size > 1000000000) {
            return String.format("%,.2fG", (float) size / 1000000000);
        } else if (size > 1000000) {
            return String.format("%,.2fM", (float) size / 1000000);
        } else if (size > 1000) {
            return String.format("%,.2fK", (float) size / 1000);
        } else {
            return size + "B";
        }
    }


    public synchronized ConcurrentHashMap<PHASE, SumPerfRecord4Print> getPerfRecordMaps4print() {
        if (totalEndReport.size() > 0) {
            sumPerf4EndPrint(totalEndReport);
        }
        return perfRecordMaps4print;
    }

    public SumPerf4Report getSumPerf4Report() {
        return sumPerf4Report;
    }

    public Set<PerfRecord> getNeedReportPool4NotEnd() {
        return needReportPool4NotEnd;
    }

    public List<PerfRecord> getTotalEndReport() {
        return totalEndReport;
    }

    public Map<Integer, String> getTaskDetails() {
        return taskDetails;
    }

    public boolean isEnable() {
        return enable;
    }

    public boolean isJob() {
        return isJob;
    }

    private String cluster;
    private String jobDomain;
    private String srcType;
    private String dstType;
    private String srcGuid;
    private String dstGuid;
    private Date windowStart;
    private Date windowEnd;
    private Date jobStartTime;

    public void setJobInfo(Configuration jobInfo, boolean perfReportEnable, int channelNumber) {
        try {
            this.jobInfo = jobInfo;
            if (jobInfo != null && perfReportEnable) {

                cluster = jobInfo.getString("cluster");

                String srcDomain = jobInfo.getString("srcDomain", "null");
                String dstDomain = jobInfo.getString("dstDomain", "null");
                jobDomain = srcDomain + "|" + dstDomain;
                srcType = jobInfo.getString("srcType");
                dstType = jobInfo.getString("dstType");
                srcGuid = jobInfo.getString("srcGuid");
                dstGuid = jobInfo.getString("dstGuid");
                windowStart = getWindow(jobInfo.getString("windowStart"), true);
                windowEnd = getWindow(jobInfo.getString("windowEnd"), false);
                String jobIdStr = jobInfo.getString("jobId");
                jobId = StringUtils.isEmpty(jobIdStr) ? (long) -5 : Long.parseLong(jobIdStr);
                String jobVersionStr = jobInfo.getString("jobVersion");
                jobVersion = StringUtils.isEmpty(jobVersionStr) ? (long) -4 : Long.parseLong(jobVersionStr);
                jobStartTime = new Date();
            }
            this.perfReportEnable = perfReportEnable;
            this.channelNumber = channelNumber;
        } catch (Exception e) {
            this.perfReportEnable = false;
        }
    }

    private Date getWindow(String windowStr, boolean startWindow) {
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd 00:00:00");
        if (StringUtils.isNotEmpty(windowStr)) {
            try {
                return sdf1.parse(windowStr);
            } catch (ParseException e) {
                // do nothing
            }
        }

        if (startWindow) {
            try {
                return sdf2.parse(sdf2.format(new Date()));
            } catch (ParseException e1) {
                //do nothing
            }
        }

        return null;
    }

    public long getInstId() {
        return instId;
    }

    public Configuration getJobInfo() {
        return jobInfo;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public synchronized JobStatisticsDto2 getReports(String mode) {

        try {
            if (!enable || !perfReportEnable) {
                return null;
            }

            if (("job".equalsIgnoreCase(mode) && !isJob) || "tg".equalsIgnoreCase(mode) && isJob) {
                return null;
            }

            //每次将未完成的task的统计清空
            sumPerf4Report4NotEnd = new SumPerf4Report();
            Set<PerfRecord> needReportPool4NotEndTmp = null;
            synchronized (needReportPool4NotEnd) {
                needReportPool4NotEndTmp = new HashSet<PerfRecord>(needReportPool4NotEnd);
            }

            long curNanoTime = System.nanoTime();
            for (PerfRecord perfRecord : needReportPool4NotEndTmp) {
                sumPerf4Report4NotEnd.add(curNanoTime, perfRecord);
            }

            JobStatisticsDto2 jdo = new JobStatisticsDto2();
            jdo.setInstId(this.instId);
            if (isJob) {
                jdo.setTaskGroupId(-6);
            } else {
                jdo.setTaskGroupId(this.taskGroupId);
            }
            jdo.setJobId(this.jobId);
            jdo.setJobVersion(this.jobVersion);
            jdo.setWindowStart(this.windowStart);
            jdo.setWindowEnd(this.windowEnd);
            jdo.setJobStartTime(jobStartTime);
            jdo.setJobRunTimeMs(System.currentTimeMillis() - jobStartTime.getTime());
            jdo.setChannelNum(this.channelNumber);
            jdo.setCluster(this.cluster);
            jdo.setJobDomain(this.jobDomain);
            jdo.setSrcType(this.srcType);
            jdo.setDstType(this.dstType);
            jdo.setSrcGuid(this.srcGuid);
            jdo.setDstGuid(this.dstGuid);
            jdo.setHostAddress(HostUtils.IP);

            //sum
            jdo.setTaskTotalTimeMs(sumPerf4Report4NotEnd.totalTaskRunTimeInMs + sumPerf4Report.totalTaskRunTimeInMs);
            jdo.setOdpsBlockCloseTimeMs(sumPerf4Report4NotEnd.odpsCloseTimeInMs + sumPerf4Report.odpsCloseTimeInMs);
            jdo.setSqlQueryTimeMs(sumPerf4Report4NotEnd.sqlQueryTimeInMs + sumPerf4Report.sqlQueryTimeInMs);
            jdo.setResultNextTimeMs(sumPerf4Report4NotEnd.resultNextTimeInMs + sumPerf4Report.resultNextTimeInMs);

            return jdo;
        } catch (Exception e) {
            // do nothing
        }

        return null;
    }

    private void sumPerf4EndPrint(List<PerfRecord> totalEndReport) {
        if (!enable || totalEndReport == null) {
            return;
        }

        for (PerfRecord perfRecord : totalEndReport) {
            perfRecordMaps4print.putIfAbsent(perfRecord.getPhase(), new SumPerfRecord4Print());
            perfRecordMaps4print.get(perfRecord.getPhase()).add(perfRecord);
        }

        totalEndReport.clear();
    }

    public void setChannelNumber(int needChannelNumber) {
        this.channelNumber = needChannelNumber;
    }


    public static class SumPerf4Report {
        long totalTaskRunTimeInMs = 0L;
        long odpsCloseTimeInMs = 0L;
        long sqlQueryTimeInMs = 0L;
        long resultNextTimeInMs = 0L;

        public void add(long curNanoTime,PerfRecord perfRecord) {
            try {
                long runTimeEndInMs;
                if (perfRecord.getElapsedTimeInNs() == -1) {
                    runTimeEndInMs = (curNanoTime - perfRecord.getStartTimeInNs()) / 1000000;
                } else {
                    runTimeEndInMs = perfRecord.getElapsedTimeInNs() / 1000000;
                }
                switch (perfRecord.getPhase()) {
                    case TASK_TOTAL:
                        totalTaskRunTimeInMs += runTimeEndInMs;
                        break;
                    case SQL_QUERY:
                        sqlQueryTimeInMs += runTimeEndInMs;
                        break;
                    case RESULT_NEXT_ALL:
                        resultNextTimeInMs += runTimeEndInMs;
                        break;
                    case ODPS_BLOCK_CLOSE:
                        odpsCloseTimeInMs += runTimeEndInMs;
                        break;
                }
            }catch (Exception e){
                //do nothing
            }
        }

        public long getTotalTaskRunTimeInMs() {
            return totalTaskRunTimeInMs;
        }

        public long getOdpsCloseTimeInMs() {
            return odpsCloseTimeInMs;
        }

        public long getSqlQueryTimeInMs() {
            return sqlQueryTimeInMs;
        }

        public long getResultNextTimeInMs() {
            return resultNextTimeInMs;
        }
    }

    public static class SumPerfRecord4Print {
        private long perfTimeTotal = 0;
        private long averageTime = 0;
        private long maxTime = 0;
        private int maxTaskId = -1;
        private int maxTaskGroupId = -1;
        private int totalCount = 0;

        private long recordsTotal = 0;
        private long sizesTotal = 0;
        private long averageRecords = 0;
        private long averageBytes = 0;
        private long maxRecord = 0;
        private long maxByte = 0;
        private int maxTaskId4Records = -1;
        private int maxTGID4Records = -1;

        public void add(PerfRecord perfRecord) {
            if (perfRecord == null) {
                return;
            }
            perfTimeTotal += perfRecord.getElapsedTimeInNs();
            if (perfRecord.getElapsedTimeInNs() >= maxTime) {
                maxTime = perfRecord.getElapsedTimeInNs();
                maxTaskId = perfRecord.getTaskId();
                maxTaskGroupId = perfRecord.getTaskGroupId();
            }

            recordsTotal += perfRecord.getCount();
            sizesTotal += perfRecord.getSize();
            if (perfRecord.getCount() >= maxRecord) {
                maxRecord = perfRecord.getCount();
                maxByte = perfRecord.getSize();
                maxTaskId4Records = perfRecord.getTaskId();
                maxTGID4Records = perfRecord.getTaskGroupId();
            }

            totalCount++;
        }

        public long getPerfTimeTotal() {
            return perfTimeTotal;
        }

        public long getAverageTime() {
            if (totalCount > 0) {
                averageTime = perfTimeTotal / totalCount;
            }
            return averageTime;
        }

        public long getMaxTime() {
            return maxTime;
        }

        public int getMaxTaskId() {
            return maxTaskId;
        }

        public int getMaxTaskGroupId() {
            return maxTaskGroupId;
        }

        public long getRecordsTotal() {
            return recordsTotal;
        }

        public long getSizesTotal() {
            return sizesTotal;
        }

        public long getAverageRecords() {
            if (totalCount > 0) {
                averageRecords = recordsTotal / totalCount;
            }
            return averageRecords;
        }

        public long getAverageBytes() {
            if (totalCount > 0) {
                averageBytes = sizesTotal / totalCount;
            }
            return averageBytes;
        }

        public long getMaxRecord() {
            return maxRecord;
        }

        public long getMaxByte() {
            return maxByte;
        }

        public int getMaxTaskId4Records() {
            return maxTaskId4Records;
        }

        public int getMaxTGID4Records() {
            return maxTGID4Records;
        }

        public int getTotalCount() {
            return totalCount;
        }
    }
    class JobStatisticsDto2 {

        private Long id;
        private Date gmtCreate;
        private Date gmtModified;
        private Long instId;
        private Long jobId;
        private Long jobVersion;
        private Integer taskGroupId;
        private Date windowStart;
        private Date windowEnd;
        private Date jobStartTime;
        private Date jobEndTime;
        private Long jobRunTimeMs;
        private Integer channelNum;
        private String cluster;
        private String jobDomain;
        private String srcType;
        private String dstType;
        private String srcGuid;
        private String dstGuid;
        private Long records;
        private Long bytes;
        private Long speedRecord;
        private Long speedByte;
        private String stagePercent;
        private Long errorRecord;
        private Long errorBytes;
        private Long waitReadTimeMs;
        private Long waitWriteTimeMs;
        private Long odpsBlockCloseTimeMs;
        private Long sqlQueryTimeMs;
        private Long resultNextTimeMs;
        private Long taskTotalTimeMs;
        private String hostAddress;

        public Long getId() {
            return id;
        }

        public Date getGmtCreate() {
            return gmtCreate;
        }

        public Date getGmtModified() {
            return gmtModified;
        }

        public Long getInstId() {
            return instId;
        }

        public Long getJobId() {
            return jobId;
        }

        public Long getJobVersion() {
            return jobVersion;
        }

        public Integer getTaskGroupId() {
            return taskGroupId;
        }

        public Date getWindowStart() {
            return windowStart;
        }

        public Date getWindowEnd() {
            return windowEnd;
        }

        public Date getJobStartTime() {
            return jobStartTime;
        }

        public Date getJobEndTime() {
            return jobEndTime;
        }

        public Long getJobRunTimeMs() {
            return jobRunTimeMs;
        }

        public Integer getChannelNum() {
            return channelNum;
        }

        public String getCluster() {
            return cluster;
        }

        public String getJobDomain() {
            return jobDomain;
        }

        public String getSrcType() {
            return srcType;
        }

        public String getDstType() {
            return dstType;
        }

        public String getSrcGuid() {
            return srcGuid;
        }

        public String getDstGuid() {
            return dstGuid;
        }

        public Long getRecords() {
            return records;
        }

        public Long getBytes() {
            return bytes;
        }

        public Long getSpeedRecord() {
            return speedRecord;
        }

        public Long getSpeedByte() {
            return speedByte;
        }

        public String getStagePercent() {
            return stagePercent;
        }

        public Long getErrorRecord() {
            return errorRecord;
        }

        public Long getErrorBytes() {
            return errorBytes;
        }

        public Long getWaitReadTimeMs() {
            return waitReadTimeMs;
        }

        public Long getWaitWriteTimeMs() {
            return waitWriteTimeMs;
        }

        public Long getOdpsBlockCloseTimeMs() {
            return odpsBlockCloseTimeMs;
        }

        public Long getSqlQueryTimeMs() {
            return sqlQueryTimeMs;
        }

        public Long getResultNextTimeMs() {
            return resultNextTimeMs;
        }

        public Long getTaskTotalTimeMs() {
            return taskTotalTimeMs;
        }

        public String getHostAddress() {
            return hostAddress;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public void setGmtCreate(Date gmtCreate) {
            this.gmtCreate = gmtCreate;
        }

        public void setGmtModified(Date gmtModified) {
            this.gmtModified = gmtModified;
        }

        public void setInstId(Long instId) {
            this.instId = instId;
        }

        public void setJobId(Long jobId) {
            this.jobId = jobId;
        }

        public void setJobVersion(Long jobVersion) {
            this.jobVersion = jobVersion;
        }

        public void setTaskGroupId(Integer taskGroupId) {
            this.taskGroupId = taskGroupId;
        }

        public void setWindowStart(Date windowStart) {
            this.windowStart = windowStart;
        }

        public void setWindowEnd(Date windowEnd) {
            this.windowEnd = windowEnd;
        }

        public void setJobStartTime(Date jobStartTime) {
            this.jobStartTime = jobStartTime;
        }

        public void setJobEndTime(Date jobEndTime) {
            this.jobEndTime = jobEndTime;
        }

        public void setJobRunTimeMs(Long jobRunTimeMs) {
            this.jobRunTimeMs = jobRunTimeMs;
        }

        public void setChannelNum(Integer channelNum) {
            this.channelNum = channelNum;
        }

        public void setCluster(String cluster) {
            this.cluster = cluster;
        }

        public void setJobDomain(String jobDomain) {
            this.jobDomain = jobDomain;
        }

        public void setSrcType(String srcType) {
            this.srcType = srcType;
        }

        public void setDstType(String dstType) {
            this.dstType = dstType;
        }

        public void setSrcGuid(String srcGuid) {
            this.srcGuid = srcGuid;
        }

        public void setDstGuid(String dstGuid) {
            this.dstGuid = dstGuid;
        }

        public void setRecords(Long records) {
            this.records = records;
        }

        public void setBytes(Long bytes) {
            this.bytes = bytes;
        }

        public void setSpeedRecord(Long speedRecord) {
            this.speedRecord = speedRecord;
        }

        public void setSpeedByte(Long speedByte) {
            this.speedByte = speedByte;
        }

        public void setStagePercent(String stagePercent) {
            this.stagePercent = stagePercent;
        }

        public void setErrorRecord(Long errorRecord) {
            this.errorRecord = errorRecord;
        }

        public void setErrorBytes(Long errorBytes) {
            this.errorBytes = errorBytes;
        }

        public void setWaitReadTimeMs(Long waitReadTimeMs) {
            this.waitReadTimeMs = waitReadTimeMs;
        }

        public void setWaitWriteTimeMs(Long waitWriteTimeMs) {
            this.waitWriteTimeMs = waitWriteTimeMs;
        }

        public void setOdpsBlockCloseTimeMs(Long odpsBlockCloseTimeMs) {
            this.odpsBlockCloseTimeMs = odpsBlockCloseTimeMs;
        }

        public void setSqlQueryTimeMs(Long sqlQueryTimeMs) {
            this.sqlQueryTimeMs = sqlQueryTimeMs;
        }

        public void setResultNextTimeMs(Long resultNextTimeMs) {
            this.resultNextTimeMs = resultNextTimeMs;
        }

        public void setTaskTotalTimeMs(Long taskTotalTimeMs) {
            this.taskTotalTimeMs = taskTotalTimeMs;
        }

        public void setHostAddress(String hostAddress) {
            this.hostAddress = hostAddress;
        }
    }
}
