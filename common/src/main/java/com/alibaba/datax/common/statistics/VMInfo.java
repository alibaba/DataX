package com.alibaba.datax.common.statistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by liqiang on 15/11/12.
 */
public class VMInfo {
    private static final Logger LOG = LoggerFactory.getLogger(VMInfo.class);
    static final long MB = 1024 * 1024;
    static final long GB = 1024 * 1024 * 1024;
    public static Object lock = new Object();
    private static VMInfo vmInfo;

    /**
     * @return null or vmInfo. null is something error, job no care it.
     */
    public static VMInfo getVmInfo() {
        if (vmInfo == null) {
            synchronized (lock) {
                if (vmInfo == null) {
                    try {
                        vmInfo = new VMInfo();
                    } catch (Exception e) {
                        LOG.warn("no need care, the fail is ignored : vmInfo init failed " + e.getMessage(), e);
                    }
                }
            }

        }
        return vmInfo;
    }

    // 数据的MxBean
    private final OperatingSystemMXBean osMXBean;
    private final RuntimeMXBean runtimeMXBean;
    private final List<GarbageCollectorMXBean> garbageCollectorMXBeanList;
    private final List<MemoryPoolMXBean> memoryPoolMXBeanList;
    /**
     * 静态信息
     */
    private final String osInfo;
    private final String jvmInfo;

    /**
     * cpu个数
     */
    private final int totalProcessorCount;

    /**
     * 机器的各个状态，用于中间打印和统计上报
     */
    private final PhyOSStatus startPhyOSStatus;
    private final ProcessCpuStatus processCpuStatus = new ProcessCpuStatus();
    private final ProcessGCStatus processGCStatus = new ProcessGCStatus();
    private final ProcessMemoryStatus processMomoryStatus = new ProcessMemoryStatus();
    //ms
    private long lastUpTime = 0;
    //nano
    private long lastProcessCpuTime = 0;


    private VMInfo() {
        //初始化静态信息
        osMXBean = java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        runtimeMXBean = java.lang.management.ManagementFactory.getRuntimeMXBean();
        garbageCollectorMXBeanList = java.lang.management.ManagementFactory.getGarbageCollectorMXBeans();
        memoryPoolMXBeanList = java.lang.management.ManagementFactory.getMemoryPoolMXBeans();

        osInfo = runtimeMXBean.getVmVendor() + " " + runtimeMXBean.getSpecVersion() + " " + runtimeMXBean.getVmVersion();
        jvmInfo = osMXBean.getName() + " " + osMXBean.getArch() + " " + osMXBean.getVersion();
        totalProcessorCount = osMXBean.getAvailableProcessors();

        //构建startPhyOSStatus
        startPhyOSStatus = new PhyOSStatus();
        LOG.info("VMInfo# operatingSystem class => " + osMXBean.getClass().getName());
        if (VMInfo.isSunOsMBean(osMXBean)) {
            {
                startPhyOSStatus.totalPhysicalMemory = VMInfo.getLongFromOperatingSystem(osMXBean, "getTotalPhysicalMemorySize");
                startPhyOSStatus.freePhysicalMemory = VMInfo.getLongFromOperatingSystem(osMXBean, "getFreePhysicalMemorySize");
                startPhyOSStatus.maxFileDescriptorCount = VMInfo.getLongFromOperatingSystem(osMXBean, "getMaxFileDescriptorCount");
                startPhyOSStatus.currentOpenFileDescriptorCount = VMInfo.getLongFromOperatingSystem(osMXBean, "getOpenFileDescriptorCount");
            }
        }

        //初始化processGCStatus;
        for (GarbageCollectorMXBean garbage : garbageCollectorMXBeanList) {
            GCStatus gcStatus = new GCStatus();
            gcStatus.name = garbage.getName();
            processGCStatus.gcStatusMap.put(garbage.getName(), gcStatus);
        }

        //初始化processMemoryStatus
        if (memoryPoolMXBeanList != null && !memoryPoolMXBeanList.isEmpty()) {
            for (MemoryPoolMXBean pool : memoryPoolMXBeanList) {
                MemoryStatus memoryStatus = new MemoryStatus();
                memoryStatus.name = pool.getName();
                memoryStatus.initSize = pool.getUsage().getInit();
                memoryStatus.maxSize = pool.getUsage().getMax();
                processMomoryStatus.memoryStatusMap.put(pool.getName(), memoryStatus);
            }
        }
    }

    public String toString() {
        return "the machine info  => \n\n"
                + "\tosInfo:\t" + osInfo + "\n"
                + "\tjvmInfo:\t" + jvmInfo + "\n"
                + "\tcpu num:\t" + totalProcessorCount + "\n\n"
                + startPhyOSStatus.toString() + "\n"
                + processGCStatus.toString() + "\n"
                + processMomoryStatus.toString() + "\n";
    }

    public String totalString() {
        return (processCpuStatus.getTotalString() + processGCStatus.getTotalString());
    }

    public void getDelta() {
        getDelta(true);
    }

    public synchronized void getDelta(boolean print) {

        try {
            if (VMInfo.isSunOsMBean(osMXBean)) {
                long curUptime = runtimeMXBean.getUptime();
                long curProcessTime = getLongFromOperatingSystem(osMXBean, "getProcessCpuTime");
                //百分比， uptime是ms，processTime是nano
                if ((curUptime > lastUpTime) && (curProcessTime >= lastProcessCpuTime)) {
                    float curDeltaCpu = (float) (curProcessTime - lastProcessCpuTime) / ((curUptime - lastUpTime) * totalProcessorCount * 10000);
                    processCpuStatus.setMaxMinCpu(curDeltaCpu);
                    processCpuStatus.averageCpu = (float) curProcessTime / (curUptime * totalProcessorCount * 10000);

                    lastUpTime = curUptime;
                    lastProcessCpuTime = curProcessTime;
                }
            }

            for (GarbageCollectorMXBean garbage : garbageCollectorMXBeanList) {

                GCStatus gcStatus = processGCStatus.gcStatusMap.get(garbage.getName());
                if (gcStatus == null) {
                    gcStatus = new GCStatus();
                    gcStatus.name = garbage.getName();
                    processGCStatus.gcStatusMap.put(garbage.getName(), gcStatus);
                }

                long curTotalGcCount = garbage.getCollectionCount();
                gcStatus.setCurTotalGcCount(curTotalGcCount);

                long curtotalGcTime = garbage.getCollectionTime();
                gcStatus.setCurTotalGcTime(curtotalGcTime);
            }

            if (memoryPoolMXBeanList != null && !memoryPoolMXBeanList.isEmpty()) {
                for (MemoryPoolMXBean pool : memoryPoolMXBeanList) {

                    MemoryStatus memoryStatus = processMomoryStatus.memoryStatusMap.get(pool.getName());
                    if (memoryStatus == null) {
                        memoryStatus = new MemoryStatus();
                        memoryStatus.name = pool.getName();
                        processMomoryStatus.memoryStatusMap.put(pool.getName(), memoryStatus);
                    }
                    memoryStatus.commitedSize = pool.getUsage().getCommitted();
                    memoryStatus.setMaxMinUsedSize(pool.getUsage().getUsed());
                    long maxMemory = memoryStatus.commitedSize > 0 ? memoryStatus.commitedSize : memoryStatus.maxSize;
                    memoryStatus.setMaxMinPercent(maxMemory > 0 ? (float) 100 * memoryStatus.usedSize / maxMemory : -1);
                }
            }

            if (print) {
                LOG.info(processCpuStatus.getDeltaString() + processMomoryStatus.getDeltaString() + processGCStatus.getDeltaString());
            }

        } catch (Exception e) {
            LOG.warn("no need care, the fail is ignored : vmInfo getDelta failed " + e.getMessage(), e);
        }
    }

    public static boolean isSunOsMBean(OperatingSystemMXBean operatingSystem) {
        final String className = operatingSystem.getClass().getName();

        return "com.sun.management.UnixOperatingSystem".equals(className);
    }

    public static long getLongFromOperatingSystem(OperatingSystemMXBean operatingSystem, String methodName) {
        try {
            final Method method = operatingSystem.getClass().getMethod(methodName, (Class<?>[]) null);
            method.setAccessible(true);
            return (Long) method.invoke(operatingSystem, (Object[]) null);
        } catch (final Exception e) {
            LOG.info(String.format("OperatingSystemMXBean %s failed, Exception = %s ", methodName, e.getMessage()));
        }

        return -1;
    }

    private class PhyOSStatus {
        long totalPhysicalMemory = -1;
        long freePhysicalMemory = -1;
        long maxFileDescriptorCount = -1;
        long currentOpenFileDescriptorCount = -1;

        public String toString() {
            return String.format("\ttotalPhysicalMemory:\t%,.2fG\n"
                            + "\tfreePhysicalMemory:\t%,.2fG\n"
                            + "\tmaxFileDescriptorCount:\t%s\n"
                            + "\tcurrentOpenFileDescriptorCount:\t%s\n",
                    (float) totalPhysicalMemory / GB, (float) freePhysicalMemory / GB, maxFileDescriptorCount, currentOpenFileDescriptorCount);
        }
    }

    private class ProcessCpuStatus {
        // 百分比的值 比如30.0 表示30.0%
        float maxDeltaCpu = -1;
        float minDeltaCpu = -1;
        float curDeltaCpu = -1;
        float averageCpu = -1;

        public void setMaxMinCpu(float curCpu) {
            this.curDeltaCpu = curCpu;
            if (maxDeltaCpu < curCpu) {
                maxDeltaCpu = curCpu;
            }

            if (minDeltaCpu == -1 || minDeltaCpu > curCpu) {
                minDeltaCpu = curCpu;
            }
        }

        public String getDeltaString() {
            StringBuilder sb = new StringBuilder();
            sb.append("\n\t [delta cpu info] => \n");
            sb.append("\t\t");
            sb.append(String.format("%-30s | %-30s | %-30s | %-30s \n", "curDeltaCpu", "averageCpu", "maxDeltaCpu", "minDeltaCpu"));
            sb.append("\t\t");
            sb.append(String.format("%-30s | %-30s | %-30s | %-30s \n",
                    String.format("%,.2f%%", processCpuStatus.curDeltaCpu),
                    String.format("%,.2f%%", processCpuStatus.averageCpu),
                    String.format("%,.2f%%", processCpuStatus.maxDeltaCpu),
                    String.format("%,.2f%%\n", processCpuStatus.minDeltaCpu)));

            return sb.toString();
        }

        public String getTotalString() {
            StringBuilder sb = new StringBuilder();
            sb.append("\n\t [total cpu info] => \n");
            sb.append("\t\t");
            sb.append(String.format("%-30s | %-30s | %-30s \n", "averageCpu", "maxDeltaCpu", "minDeltaCpu"));
            sb.append("\t\t");
            sb.append(String.format("%-30s | %-30s | %-30s \n",
                    String.format("%,.2f%%", processCpuStatus.averageCpu),
                    String.format("%,.2f%%", processCpuStatus.maxDeltaCpu),
                    String.format("%,.2f%%\n", processCpuStatus.minDeltaCpu)));

            return sb.toString();
        }

    }

    private class ProcessGCStatus {
        final Map<String, GCStatus> gcStatusMap = new HashMap<String, GCStatus>();

        public String toString() {
            return "\tGC Names\t" + gcStatusMap.keySet() + "\n";
        }

        public String getDeltaString() {
            StringBuilder sb = new StringBuilder();
            sb.append("\n\t [delta gc info] => \n");
            sb.append("\t\t ");
            sb.append(String.format("%-20s | %-18s | %-18s | %-18s | %-18s | %-18s | %-18s | %-18s | %-18s \n", "NAME", "curDeltaGCCount", "totalGCCount", "maxDeltaGCCount", "minDeltaGCCount", "curDeltaGCTime", "totalGCTime", "maxDeltaGCTime", "minDeltaGCTime"));
            for (GCStatus gc : gcStatusMap.values()) {
                sb.append("\t\t ");
                sb.append(String.format("%-20s | %-18s | %-18s | %-18s | %-18s | %-18s | %-18s | %-18s | %-18s \n",
                        gc.name, gc.curDeltaGCCount, gc.totalGCCount, gc.maxDeltaGCCount, gc.minDeltaGCCount,
                        String.format("%,.3fs",(float)gc.curDeltaGCTime/1000),
                        String.format("%,.3fs",(float)gc.totalGCTime/1000),
                        String.format("%,.3fs",(float)gc.maxDeltaGCTime/1000),
                        String.format("%,.3fs",(float)gc.minDeltaGCTime/1000)));

            }
            return sb.toString();
        }

        public String getTotalString() {
            StringBuilder sb = new StringBuilder();
            sb.append("\n\t [total gc info] => \n");
            sb.append("\t\t ");
            sb.append(String.format("%-20s | %-18s | %-18s | %-18s | %-18s | %-18s | %-18s \n", "NAME", "totalGCCount", "maxDeltaGCCount", "minDeltaGCCount", "totalGCTime", "maxDeltaGCTime", "minDeltaGCTime"));
            for (GCStatus gc : gcStatusMap.values()) {
                sb.append("\t\t ");
                sb.append(String.format("%-20s | %-18s | %-18s | %-18s | %-18s | %-18s | %-18s \n",
                        gc.name, gc.totalGCCount, gc.maxDeltaGCCount, gc.minDeltaGCCount,
                        String.format("%,.3fs",(float)gc.totalGCTime/1000),
                        String.format("%,.3fs",(float)gc.maxDeltaGCTime/1000),
                        String.format("%,.3fs",(float)gc.minDeltaGCTime/1000)));

            }
            return sb.toString();
        }
    }

    private class ProcessMemoryStatus {
        final Map<String, MemoryStatus> memoryStatusMap = new HashMap<String, MemoryStatus>();

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("\t");
            sb.append(String.format("%-30s | %-30s | %-30s \n", "MEMORY_NAME", "allocation_size", "init_size"));
            for (MemoryStatus ms : memoryStatusMap.values()) {
                sb.append("\t");
                sb.append(String.format("%-30s | %-30s | %-30s \n",
                        ms.name, String.format("%,.2fMB", (float) ms.maxSize / MB), String.format("%,.2fMB", (float) ms.initSize / MB)));
            }
            return sb.toString();
        }

        public String getDeltaString() {
            StringBuilder sb = new StringBuilder();
            sb.append("\n\t [delta memory info] => \n");
            sb.append("\t\t ");
            sb.append(String.format("%-30s | %-30s | %-30s | %-30s | %-30s \n", "NAME", "used_size", "used_percent", "max_used_size", "max_percent"));
            for (MemoryStatus ms : memoryStatusMap.values()) {
                sb.append("\t\t ");
                sb.append(String.format("%-30s | %-30s | %-30s | %-30s | %-30s \n",
                        ms.name, String.format("%,.2f", (float) ms.usedSize / MB) + "MB",
                        String.format("%,.2f", (float) ms.percent) + "%",
                        String.format("%,.2f", (float) ms.maxUsedSize / MB) + "MB",
                        String.format("%,.2f", (float) ms.maxpercent) + "%"));

            }
            return sb.toString();
        }
    }

    private class GCStatus {
        String name;
        long maxDeltaGCCount = -1;
        long minDeltaGCCount = -1;
        long curDeltaGCCount;
        long totalGCCount = 0;
        long maxDeltaGCTime = -1;
        long minDeltaGCTime = -1;
        long curDeltaGCTime;
        long totalGCTime = 0;

        public void setCurTotalGcCount(long curTotalGcCount) {
            this.curDeltaGCCount = curTotalGcCount - totalGCCount;
            this.totalGCCount = curTotalGcCount;

            if (maxDeltaGCCount < curDeltaGCCount) {
                maxDeltaGCCount = curDeltaGCCount;
            }

            if (minDeltaGCCount == -1 || minDeltaGCCount > curDeltaGCCount) {
                minDeltaGCCount = curDeltaGCCount;
            }
        }

        public void setCurTotalGcTime(long curTotalGcTime) {
            this.curDeltaGCTime = curTotalGcTime - totalGCTime;
            this.totalGCTime = curTotalGcTime;

            if (maxDeltaGCTime < curDeltaGCTime) {
                maxDeltaGCTime = curDeltaGCTime;
            }

            if (minDeltaGCTime == -1 || minDeltaGCTime > curDeltaGCTime) {
                minDeltaGCTime = curDeltaGCTime;
            }
        }
    }

    private class MemoryStatus {
        String name;
        long initSize;
        long maxSize;
        long commitedSize;
        long usedSize;
        float percent;
        long maxUsedSize = -1;
        float maxpercent = 0;

        void setMaxMinUsedSize(long curUsedSize) {
            if (maxUsedSize < curUsedSize) {
                maxUsedSize = curUsedSize;
            }
            this.usedSize = curUsedSize;
        }

        void setMaxMinPercent(float curPercent) {
            if (maxpercent < curPercent) {
                maxpercent = curPercent;
            }
            this.percent = curPercent;
        }
    }

}
