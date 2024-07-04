package com.alibaba.datax.plugin.writer.obhbasewriter.task;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.MessageSource;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.writer.obhbasewriter.Config;
import com.alibaba.datax.plugin.writer.obhbasewriter.ConfigKey;
import com.alibaba.datax.plugin.writer.obhbasewriter.Constant;
import com.alibaba.datax.plugin.writer.obhbasewriter.NullModeType;
import com.alibaba.datax.plugin.writer.obhbasewriter.ObHTableInfo;
import com.alibaba.datax.plugin.writer.obhbasewriter.ext.ServerConnectInfo;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObHBaseWriteTask extends CommonRdbmsWriter.Task {
    private final static MessageSource MESSAGE_SOURCE = MessageSource.loadResourceBundle(ObHBaseWriteTask.class);
    private final static Logger LOG = LoggerFactory.getLogger(ObHBaseWriteTask.class);

    public NullModeType nullMode = null;
    private int maxRetryCount;

    public List<Configuration> columns;
    public List<Configuration> rowkeyColumn;
    public Configuration versionColumn;

    public String hbaseTableName;
    public String encoding;
    public Boolean walFlag;

    String configUrl;
    String dbName;
    String ip;
    String port;

    String fullUserName;
    boolean usdOdpMode;
    String sysUsername;
    String sysPassword;
    private ObHTableInfo obHTableInfo;

    private ConcurrentTableWriter concurrentWriter;
    private boolean allTaskInQueue = false;
    private long startTime = 0;
    private String threadName = Thread.currentThread().getName();

    private Lock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();

    public ObHBaseWriteTask(Configuration configuration) {
        super(DataBaseType.MySql);
        init(configuration);
    }

    @Override
    public void init(com.alibaba.datax.common.util.Configuration configuration) {
        this.obHTableInfo = new ObHTableInfo(configuration);
        this.hbaseTableName = configuration.getString(ConfigKey.TABLE);
        this.columns = configuration.getListConfiguration(ConfigKey.COLUMN);
        this.rowkeyColumn = configuration.getListConfiguration(ConfigKey.ROWKEY_COLUMN);
        this.versionColumn = configuration.getConfiguration(ConfigKey.VERSION_COLUMN);
        this.encoding = configuration.getString(ConfigKey.ENCODING, Constant.DEFAULT_ENCODING);
        this.nullMode = NullModeType.getByTypeName(configuration.getString(ConfigKey.NULL_MODE, Constant.DEFAULT_NULL_MODE));
//        this.memstoreThreshold = configuration.getDouble(Config.MEMSTORE_THRESHOLD, Config.DEFAULT_MEMSTORE_THRESHOLD);
        this.walFlag = configuration.getBool(ConfigKey.WAL_FLAG, true);
        this.maxRetryCount = configuration.getInt(ConfigKey.MAX_RETRY_COUNT, 3);

        // default 1000 rows are committed together
        this.batchSize = com.alibaba.datax.plugin.rdbms.writer.Constant.DEFAULT_BATCH_SIZE;
        this.batchByteSize = com.alibaba.datax.plugin.rdbms.writer.Constant.DEFAULT_BATCH_BYTE_SIZE;

        this.configUrl = configuration.getString(ConfigKey.OBCONFIG_URL);
        this.jdbcUrl = configuration.getString(ConfigKey.JDBC_URL);
        this.username = configuration.getString(Key.USERNAME);
        this.password = configuration.getString(Key.PASSWORD);
        this.dbName = configuration.getString(Key.DBNAME);
        this.usdOdpMode = configuration.getBool(ConfigKey.USE_ODP_MODE);

        ServerConnectInfo connectInfo = new ServerConnectInfo(jdbcUrl, username, password);
        String clusterName = connectInfo.clusterName;
        this.fullUserName = connectInfo.getFullUserName();
        final String[] ipPort = connectInfo.ipPort.split(":");
        if (usdOdpMode) {
            this.ip = ipPort[0];
            this.port = ipPort[1];
        } else {
            this.sysUsername = configuration.getString(ConfigKey.OB_SYS_USER);
            this.sysPassword = configuration.getString(ConfigKey.OB_SYS_PASSWORD);
            connectInfo.setSysUser(sysUsername);
            connectInfo.setSysPass(sysPassword);
            if (!configUrl.contains("ObRegion")) {
                if (configUrl.contains("?")) {
                    configUrl += "&ObRegion=" + clusterName;
                } else {
                    configUrl += "?ObRegion=" + clusterName;
                }
            }
            if (!configUrl.contains("database")) {
                configUrl += "&database=" + dbName;
            }
        }
        if (null == concurrentWriter) {
            concurrentWriter = new ConcurrentTableWriter(configuration, connectInfo);
            allTaskInQueue = false;
        }
    }

    @Override
    public void prepare(Configuration configuration) {
        concurrentWriter.start();
    }

    @Override
    public void startWrite(RecordReceiver recordReceiver, Configuration configuration, TaskPluginCollector taskPluginCollector) {
        this.taskPluginCollector = taskPluginCollector;
        int recordCount = 0;
        int bufferBytes = 0;
        List<Record> records = new ArrayList<>();
        try {
            Record record;
            while ((record = recordReceiver.getFromReader()) != null) {
                recordCount++;
                bufferBytes += record.getMemorySize();
                records.add(record);
                // 按照指定的批大小进行批量写入
                if (records.size() >= batchSize || bufferBytes >= batchByteSize) {
                    concurrentWriter.addBatchRecords(Lists.newArrayList(records));
                    records.clear();
                    bufferBytes = 0;
                }
            }

            if (!records.isEmpty()) {
                concurrentWriter.addBatchRecords(records);
            }
        } catch (Throwable e) {
            LOG.warn("startWrite error unexpected ", e);
            throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
        }
        LOG.info(recordCount + " rows received.");
        waitTaskFinish();
    }

    public void waitTaskFinish() {
        this.allTaskInQueue = true;
        LOG.info("ConcurrentTableWriter has put all task in queue, queueSize = {},  total = {}, finished = {}",
                concurrentWriter.getTaskQueueSize(),
                concurrentWriter.getTotalTaskCount(),
                concurrentWriter.getFinishTaskCount());

        lock.lock();
        try {
            while (!concurrentWriter.checkFinish()) {
                condition.await(50, TimeUnit.MILLISECONDS);
                // print statistic
                LOG.debug("Statistic total task {}, finished {}, queue Size {}",
                        concurrentWriter.getTotalTaskCount(),
                        concurrentWriter.getFinishTaskCount(),
                        concurrentWriter.getTaskQueueSize());
                concurrentWriter.printStatistics();
            }
        } catch (InterruptedException e) {
            LOG.warn("Concurrent table writer wait task finish interrupt");
        } finally {
            lock.unlock();
        }
        LOG.debug("wait all InsertTask finished ...");
    }

    public boolean isFinished() {
        return allTaskInQueue && concurrentWriter.checkFinish();
    }

    public void singalTaskFinish() {
        lock.lock();
        try {
            condition.signal();
        } finally {
            lock.unlock();
        }
    }

    public void collectDirtyRecord(Record record, Throwable throwable) {
        this.taskPluginCollector.collectDirtyRecord(record, throwable);
    }

    @Override
    public void post(Configuration configuration) {

    }

    @Override
    public void destroy(Configuration configuration) {
        if (concurrentWriter != null) {
            concurrentWriter.destory();
        }
        super.destroy(configuration);
    }

    public class ConcurrentTableWriter {
        private BlockingQueue<List<Record>> queue;
        private List<PutTask> putTasks;
        private Configuration config;
        private AtomicLong totalTaskCount;
        private AtomicLong finishTaskCount;
        private ServerConnectInfo connectInfo;
        private ExecutorService executorService;
        private final int threadCount;

        public ConcurrentTableWriter(Configuration config, ServerConnectInfo connectInfo) {
            this.threadCount = config.getInt(Config.WRITER_THREAD_COUNT, Config.DEFAULT_WRITER_THREAD_COUNT);
            this.queue = new LinkedBlockingQueue<List<Record>>(threadCount << 1);
            this.putTasks = new ArrayList<PutTask>(threadCount);
            this.config = config;
            this.totalTaskCount = new AtomicLong(0);
            this.finishTaskCount = new AtomicLong(0);
            this.executorService = Executors.newFixedThreadPool(threadCount);
            this.connectInfo = connectInfo;
        }

        public long getTotalTaskCount() {
            return totalTaskCount.get();
        }

        public long getFinishTaskCount() {
            return finishTaskCount.get();
        }

        public int getTaskQueueSize() {
            return queue.size();
        }

        public void increFinishCount() {
            finishTaskCount.incrementAndGet();
        }

        // should check after put all the task in the queue
        public boolean checkFinish() {
            long finishCount = finishTaskCount.get();
            long totalCount = totalTaskCount.get();
            return finishCount == totalCount;
        }

        public synchronized void start() {
            for (int i = 0; i < threadCount; ++i) {
                LOG.info("start {} insert task.", (i + 1));
                PutTask putTask = new PutTask(threadName, queue, config, connectInfo, obHTableInfo, ObHBaseWriteTask.this);
                putTask.setWriter(this);
                putTasks.add(putTask);
            }
            for (PutTask task : putTasks) {
                executorService.execute(task);
            }
        }

        public void printStatistics() {
            long insertTotalCost = 0;
            long insertTotalCount = 0;
            for (PutTask task : putTasks) {
                insertTotalCost += task.getTotalCost();
                insertTotalCount += task.getPutCount();
            }
            long avgCost = 0;
            if (insertTotalCount != 0) {
                avgCost = insertTotalCost / insertTotalCount;
            }
            ObHBaseWriteTask.LOG.debug("Put {} times, totalCost {} ms, average {} ms",
                    insertTotalCount, insertTotalCost, avgCost);
        }

        public void addBatchRecords(final List<Record> records) throws InterruptedException {
            boolean isSucc = false;
            while (!isSucc) {
                isSucc = queue.offer(records, 5, TimeUnit.MILLISECONDS);
            }
            totalTaskCount.incrementAndGet();
        }

        public synchronized void destory() {
            if (putTasks != null) {
                for (PutTask task : putTasks) {
                    task.setStop();
                    task.destroy();
                }
            }
            destroyExecutor();
        }

        private void destroyExecutor() {
            if (executorService != null && !executorService.isShutdown()) {
                executorService.shutdown();
                try {
                    executorService.awaitTermination(0L, TimeUnit.SECONDS);
                } catch (InterruptedException var2) {
                }
            }
        }
    }
}
