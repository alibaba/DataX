package com.alibaba.datax.plugin.writer.oceanbasev10writer.task;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.Config;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.ext.AbstractConnHolder;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.ext.ObClientConnHolder;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.ext.ServerConnectInfo;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.part.IObPartCalculator;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.part.ObPartitionCalculatorV1;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.part.ObPartitionCalculatorV2;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.util.ObWriterUtils;

import com.oceanbase.partition.calculator.enums.ObServerMode;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.datax.plugin.writer.oceanbasev10writer.Config.DEFAULT_SLOW_MEMSTORE_THRESHOLD;
import static com.alibaba.datax.plugin.writer.oceanbasev10writer.util.ObWriterUtils.LoadMode.FAST;
import static com.alibaba.datax.plugin.writer.oceanbasev10writer.util.ObWriterUtils.LoadMode.PAUSE;
import static com.alibaba.datax.plugin.writer.oceanbasev10writer.util.ObWriterUtils.LoadMode.SLOW;

public class ConcurrentTableWriterTask extends CommonRdbmsWriter.Task {
    private static final Logger LOG = LoggerFactory.getLogger(ConcurrentTableWriterTask.class);

    // memstore_total 与 memstore_limit 比例的阈值,一旦超过这个值,则暂停写入
    private double memstoreThreshold = Config.DEFAULT_MEMSTORE_THRESHOLD;
    // memstore检查的间隔
    private long memstoreCheckIntervalSecond = Config.DEFAULT_MEMSTORE_CHECK_INTERVAL_SECOND;
    // 最后一次检查
    private long lastCheckMemstoreTime;

    private volatile ObWriterUtils.LoadMode loadMode = FAST;

    private static AtomicLong totalTask = new AtomicLong(0);
    private long taskId = -1;
    private AtomicBoolean isMemStoreFull = new AtomicBoolean(false);
    private HashMap<Long, List<Record>> groupInsertValues;
    private IObPartCalculator obPartCalculator;
    private ConcurrentTableWriter concurrentWriter = null;
    private AbstractConnHolder connHolder;
    private boolean allTaskInQueue = false;
    private Lock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();
    private long startTime;
    private String obWriteMode = "update";
    private boolean isOracleCompatibleMode = false;
    private String obUpdateColumns = null;
    private String dbName;
    private int calPartFailedCount = 0;
    private boolean directPath;

    public ConcurrentTableWriterTask(DataBaseType dataBaseType) {
        super(dataBaseType);
        taskId = totalTask.getAndIncrement();
    }

    @Override
    public void init(Configuration config) {
        super.init(config);
        this.directPath = config.getBool(Config.DIRECT_PATH, false);
        // OceanBase 所有操作都是 insert into on duplicate key update 模式
        // writeMode应该使用enum来定义
        this.writeMode = "update";
        obWriteMode = config.getString(Config.OB_WRITE_MODE, "update");
        ServerConnectInfo connectInfo = new ServerConnectInfo(jdbcUrl, username, password, config);
        connectInfo.setRpcPort(config.getInt(Config.RPC_PORT, 0));
        dbName = connectInfo.databaseName;
        //init check memstore
        this.memstoreThreshold = config.getDouble(Config.MEMSTORE_THRESHOLD, Config.DEFAULT_MEMSTORE_THRESHOLD);
        this.memstoreCheckIntervalSecond = config.getLong(Config.MEMSTORE_CHECK_INTERVAL_SECOND,
                Config.DEFAULT_MEMSTORE_CHECK_INTERVAL_SECOND);

        this.connHolder = new ObClientConnHolder(config, connectInfo.jdbcUrl,
                connectInfo.getFullUserName(), connectInfo.password);
        this.isOracleCompatibleMode = ObWriterUtils.isOracleMode();
        if (isOracleCompatibleMode) {
            connectInfo.databaseName = connectInfo.databaseName.toUpperCase();
            //在转义的情况下不翻译
            if (!(table.startsWith("\"") && table.endsWith("\""))) {
                table = table.toUpperCase();
            }

            LOG.info(String.format("this is oracle compatible mode, change database to %s, table to %s",
                    connectInfo.databaseName, table));
        }

        if (config.getBool(Config.USE_PART_CALCULATOR, Config.DEFAULT_USE_PART_CALCULATOR)) {
            this.obPartCalculator = createPartitionCalculator(connectInfo, ObServerMode.from(config.getString(Config.OB_COMPATIBLE_MODE), config.getString(Config.OB_VERSION)));
        } else {
            LOG.info("Disable partition calculation feature.");
        }

        obUpdateColumns = config.getString(Config.OB_UPDATE_COLUMNS, null);
        groupInsertValues = new HashMap<Long, List<Record>>();
        rewriteSql();

        if (null == concurrentWriter) {
            concurrentWriter = new ConcurrentTableWriter(config, connectInfo, writeRecordSql);
            allTaskInQueue = false;
        }
    }

    /**
     * 创建需要的分区计算组件
     *
     * @param connectInfo
     * @return
     */
    private IObPartCalculator createPartitionCalculator(ServerConnectInfo connectInfo, ObServerMode obServerMode) {
        if (obServerMode.isSubsequentFrom("3.0.0.0")) {
            LOG.info("oceanbase version is {}, use ob-partition-calculator to calculate partition Id.", obServerMode.getVersion());
            return new ObPartitionCalculatorV2(connectInfo, table, obServerMode, columns);
        }

        LOG.info("oceanbase version is {}, use ocj to calculate partition Id.", obServerMode.getVersion());
        return new ObPartitionCalculatorV1(connectInfo, table, columns);
    }

    public boolean isFinished() {
        return allTaskInQueue && concurrentWriter.checkFinish();
    }

    public boolean allTaskInQueue() {
        return allTaskInQueue;
    }

    public void setPutAllTaskInQueue() {
        this.allTaskInQueue = true;
        LOG.info("ConcurrentTableWriter has put all task in queue, queueSize = {},  total = {}, finished = {}",
                concurrentWriter.getTaskQueueSize(),
                concurrentWriter.getTotalTaskCount(),
                concurrentWriter.getFinishTaskCount());
    }

    private void rewriteSql() {
        Connection conn = connHolder.initConnection();
        if (isOracleCompatibleMode && obWriteMode.equalsIgnoreCase("update")) {
            // change obWriteMode to insert so the insert statement will be generated.
            obWriteMode = "insert";
        }
        this.writeRecordSql = ObWriterUtils.buildWriteSql(table, columns, conn, obWriteMode, obUpdateColumns);
        LOG.info("writeRecordSql :{}", this.writeRecordSql);
    }

    @Override
    public void prepare(Configuration writerSliceConfig) {
        super.prepare(writerSliceConfig);
        concurrentWriter.start();
    }

    @Override
    public void startWriteWithConnection(RecordReceiver recordReceiver, TaskPluginCollector taskPluginCollector, Connection connection) {
        this.taskPluginCollector = taskPluginCollector;

        // 用于写入数据的时候的类型根据目的表字段类型转换
        int retryTimes = 0;
        boolean needRetry = false;
        do {
            try {
                if (retryTimes > 0) {
                    TimeUnit.SECONDS.sleep((1 << retryTimes));
                    DBUtil.closeDBResources(null, connection);
                    connection = DBUtil.getConnection(dataBaseType, jdbcUrl, username, password);
                    LOG.warn("getColumnMetaData of table {} failed, retry the {} times ...", this.table, retryTimes);
                }
                ColumnMetaCache.init(connection, this.table, this.columns);
                this.resultSetMetaData = ColumnMetaCache.getColumnMeta();
                needRetry = false;
            } catch (SQLException e) {
                needRetry = true;
                ++retryTimes;
                e.printStackTrace();
                LOG.warn("fetch column meta of [{}] failed..., retry {} times", this.table, retryTimes);
            } catch (InterruptedException e) {
                LOG.warn("startWriteWithConnection interrupt, ignored");
            } finally {
            }
        } while (needRetry && retryTimes < 100);

        try {
            Record record;
            startTime = System.currentTimeMillis();
            while ((record = recordReceiver.getFromReader()) != null) {
                if (record.getColumnNumber() != this.columnNumber) {
                    // 源头读取字段列数与目的表字段写入列数不相等，直接报错
                    LOG.error("column not equal {} != {}, record = {}",
                            this.columnNumber, record.getColumnNumber(), record.toString());
                    throw DataXException
                            .asDataXException(
                                    DBUtilErrorCode.CONF_ERROR,
                                    String.format("Recoverable exception in OB. Roll back this write and hibernate for one minute. SQLState: %d. ErrorCode: %d",
                                            record.getColumnNumber(),
                                            this.columnNumber));
                }
                addRecordToCache(record);
            }
            addLeftRecords();
            waitTaskFinish();
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.WRITE_DATA_ERROR, e);
        } finally {
            DBUtil.closeDBResources(null, null, connection);
        }
    }

    public PreparedStatement fillStatement(PreparedStatement preparedStatement, Record record)
            throws SQLException {
        return fillPreparedStatement(preparedStatement, record);
    }

    private void addLeftRecords() {
        //不需要刷新Cache，已经是最后一批数据了
        for (List<Record> groupValues : groupInsertValues.values()) {
            if (groupValues.size() > 0) {
                addRecordsToWriteQueue(groupValues);
            }
        }
    }

    private void addRecordToCache(final Record record) {
        Long partId = null;
        try {
            partId = obPartCalculator == null ? Long.MAX_VALUE : obPartCalculator.calculate(record);
        } catch (Exception e1) {
            if (calPartFailedCount++ < 10) {
                LOG.warn("fail to get partition id: " + e1.getMessage() + ", record: " + record);
            }
        }

        if (partId == null) {
            LOG.debug("fail to calculate parition id, just put into the default buffer.");
            partId = Long.MAX_VALUE;
        }

        List<Record> groupValues = groupInsertValues.computeIfAbsent(partId, k -> new ArrayList<Record>(batchSize));
        groupValues.add(record);
        if (groupValues.size() >= batchSize) {
            groupValues = addRecordsToWriteQueue(groupValues);
            groupInsertValues.put(partId, groupValues);
        }
    }

    /**
     * @param records
     * @return 返回一个新的Cache用于存储接下来的数据
     */
    private List<Record> addRecordsToWriteQueue(List<Record> records) {
        int i = 0;
        while (true) {
            if (i > 0) {
                LOG.info("retry add batch record the {} times", i);
            }
            try {
                concurrentWriter.addBatchRecords(records);
                break;
            } catch (InterruptedException e) {
                i++;
                LOG.info("Concurrent table writer is interrupted");
            }
        }
        return new ArrayList<Record>(batchSize);
    }

    private void checkMemStore() {
        Connection checkConn = connHolder.getConn();
        try {
            if (checkConn == null || checkConn.isClosed()) {
                checkConn = connHolder.reconnect();
            }
        } catch (Exception e) {
            LOG.warn("Check connection is unusable");
        }

        long now = System.currentTimeMillis();
        if (now - lastCheckMemstoreTime < 1000 * memstoreCheckIntervalSecond) {
            return;
        }
        double memUsedRatio = ObWriterUtils.queryMemUsedRatio(checkConn);
        if (memUsedRatio >= DEFAULT_SLOW_MEMSTORE_THRESHOLD) {
            this.loadMode = memUsedRatio >= memstoreThreshold ? PAUSE : SLOW;
            LOG.info("Memstore used ration is {}. Load data {}", memUsedRatio, loadMode.name());
        } else {
            this.loadMode = FAST;
        }
        lastCheckMemstoreTime = now;
    }

    public boolean isMemStoreFull() {
        return isMemStoreFull.get();
    }

    public boolean isShouldPause() {
        return this.loadMode.equals(PAUSE);
    }

    public boolean isShouldSlow() {
        return this.loadMode.equals(SLOW);
    }

    public void print() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Statistic total task {}, finished {}, queue Size {}",
                    concurrentWriter.getTotalTaskCount(),
                    concurrentWriter.getFinishTaskCount(),
                    concurrentWriter.getTaskQueueSize());
            concurrentWriter.printStatistics();
        }
    }

    public void waitTaskFinish() {
        setPutAllTaskInQueue();
        lock.lock();
        try {
            while (!concurrentWriter.checkFinish()) {
                condition.await(15, TimeUnit.SECONDS);
                print();
                checkMemStore();
            }
            if (directPath){
                concurrentWriter.doCommit();
            }
        } catch (InterruptedException e) {
            LOG.warn("Concurrent table writer wait task finish interrupt");
        } finally {
            lock.unlock();
        }
        LOG.debug("wait all InsertTask finished ...");
    }

    public void singalTaskFinish() {
        lock.lock();
        condition.signal();
        lock.unlock();
    }

    @Override
    public void destroy(Configuration writerSliceConfig) {
        if (concurrentWriter != null) {
            concurrentWriter.destory();
        }
        // 把本级持有的conn关闭掉
        DBUtil.closeDBResources(null, connHolder.getConn());
        super.destroy(writerSliceConfig);
    }

    public class ConcurrentTableWriter {
        private BlockingQueue<List<Record>> queue;
        private List<AbstractInsertTask> abstractInsertTasks;
        private Configuration config;
        private ServerConnectInfo connectInfo;
        private String rewriteRecordSql;
        private AtomicLong totalTaskCount;
        private AtomicLong finishTaskCount;
        private final int threadCount;

        public ConcurrentTableWriter(Configuration config, ServerConnectInfo connInfo, String rewriteRecordSql) {
            threadCount = config.getInt(Config.WRITER_THREAD_COUNT, Config.DEFAULT_WRITER_THREAD_COUNT);
            queue = new LinkedBlockingQueue<List<Record>>(threadCount << 1);
            abstractInsertTasks = new ArrayList<AbstractInsertTask>(threadCount);
            this.config = config;
            this.connectInfo = connInfo;
            this.rewriteRecordSql = rewriteRecordSql;
            this.totalTaskCount = new AtomicLong(0);
            this.finishTaskCount = new AtomicLong(0);
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

        //should check after put all the task in the queue
        public boolean checkFinish() {
            long finishCount = finishTaskCount.get();
            long totalCount = totalTaskCount.get();
            return finishCount == totalCount;
        }

        public synchronized void start() {
            for (int i = 0; i < threadCount; ++i) {
                LOG.info("start {} insert task.", (i + 1));
                AbstractInsertTask insertTask = null;
                if (directPath) {
                    insertTask = new DirectPathInsertTask(taskId, queue, config, connectInfo, ConcurrentTableWriterTask.this, this);
                } else {
                    insertTask = new InsertTask(taskId, queue, config, connectInfo, rewriteRecordSql);
                }
                insertTask.setWriterTask(ConcurrentTableWriterTask.this);
                insertTask.setWriter(this);
                abstractInsertTasks.add(insertTask);
            }
            WriterThreadPool.executeBatch(abstractInsertTasks);
        }

        public void doCommit() {
            this.abstractInsertTasks.get(0).getConnHolder().doCommit();
        }

        public int getThreadCount() {
            return threadCount;
        }

        public void printStatistics() {
            long insertTotalCost = 0;
            long insertTotalCount = 0;
            for (AbstractInsertTask task : abstractInsertTasks) {
                insertTotalCost += task.getTotalCost();
                insertTotalCount += task.getInsertCount();
            }
            long avgCost = 0;
            if (insertTotalCount != 0) {
                avgCost = insertTotalCost / insertTotalCount;
            }
            ConcurrentTableWriterTask.LOG.debug("Insert {} times, totalCost {} ms, average {} ms",
                    insertTotalCount, insertTotalCost, avgCost);
        }

        public void addBatchRecords(final List<Record> records) throws InterruptedException {
            boolean isSucc = false;
            while (!isSucc) {
                isSucc = queue.offer(records, 5, TimeUnit.MILLISECONDS);
                checkMemStore();
            }
            totalTaskCount.incrementAndGet();
        }

        public synchronized void destory() {
            if (abstractInsertTasks != null) {
                for (AbstractInsertTask task : abstractInsertTasks) {
                    task.setStop();
                }
                for (AbstractInsertTask task : abstractInsertTasks) {
                    task.destroy();
                }
            }
        }
    }

    public String getTable() {
        return table;
    }

    // 直接使用了两个类变量：columnNumber,resultSetMetaData
    protected PreparedStatement fillPreparedStatement(PreparedStatement preparedStatement, Record record)
            throws SQLException {
        for (int i = 0; i < this.columnNumber; i++) {
            int columnSqltype = this.resultSetMetaData.getMiddle().get(i);
            String typeName = this.resultSetMetaData.getRight().get(i);
            preparedStatement = fillPreparedStatementColumnType(preparedStatement, i, columnSqltype, typeName, record.getColumn(i));
        }

        return preparedStatement;
    }

    protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex,
            int columnSqltype, String typeName, Column column) throws SQLException {
        java.util.Date utilDate;
        switch (columnSqltype) {
            case Types.CHAR:
            case Types.NCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                preparedStatement.setString(columnIndex + 1, column
                        .asString());
                break;

            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
                String strValue = column.asString();
                if (emptyAsNull && "".equals(strValue)) {
                    preparedStatement.setString(columnIndex + 1, null);
                } else {
                    preparedStatement.setString(columnIndex + 1, strValue);
                }
                break;

            //tinyint is a little special in some database like mysql {boolean->tinyint(1)}
            case Types.TINYINT:
                Long longValue = column.asLong();
                if (null == longValue) {
                    preparedStatement.setString(columnIndex + 1, null);
                } else {
                    preparedStatement.setString(columnIndex + 1, longValue.toString());
                }
                break;

            // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
            case Types.DATE:
                if (typeName == null) {
                    typeName = this.resultSetMetaData.getRight().get(columnIndex);
                }

                if (typeName.equalsIgnoreCase("year")) {
                    if (column.asBigInteger() == null) {
                        preparedStatement.setString(columnIndex + 1, null);
                    } else {
                        preparedStatement.setInt(columnIndex + 1, column.asBigInteger().intValue());
                    }
                } else {
                    java.sql.Date sqlDate = null;
                    try {
                        utilDate = column.asDate();
                    } catch (DataXException e) {
                        throw new SQLException(String.format(
                                "Date 类型转换错误：[%s]", column));
                    }

                    if (null != utilDate) {
                        sqlDate = new java.sql.Date(utilDate.getTime());
                    }
                    preparedStatement.setDate(columnIndex + 1, sqlDate);
                }
                break;

            case Types.TIME:
                java.sql.Time sqlTime = null;
                try {
                    utilDate = column.asDate();
                } catch (DataXException e) {
                    throw new SQLException(String.format(
                            "TIME 类型转换错误：[%s]", column));
                }

                if (null != utilDate) {
                    sqlTime = new java.sql.Time(utilDate.getTime());
                }
                preparedStatement.setTime(columnIndex + 1, sqlTime);
                break;

            case Types.TIMESTAMP:
                java.sql.Timestamp sqlTimestamp = null;
                try {
                    utilDate = column.asDate();
                } catch (DataXException e) {
                    throw new SQLException(String.format(
                            "TIMESTAMP 类型转换错误：[%s]", column));
                }

                if (null != utilDate) {
                    sqlTimestamp = new java.sql.Timestamp(
                            utilDate.getTime());
                }
                preparedStatement.setTimestamp(columnIndex + 1, sqlTimestamp);
                break;
            case Types.VARBINARY:
            case Types.BLOB:
            case Types.LONGVARBINARY:
                preparedStatement.setBytes(columnIndex + 1, column
                        .asBytes());
                break;
            case Types.BINARY:
                String isArray = column.getRawData().toString();
                if (isArray.startsWith("[") && isArray.endsWith("]")) {
                    preparedStatement.setString(columnIndex + 1, column
                            .asString());
                } else {
                    preparedStatement.setBytes(columnIndex + 1, column
                            .asBytes());
                }
                break;
            case Types.BOOLEAN:
                preparedStatement.setBoolean(columnIndex + 1, column.asBoolean());
                break;

            // warn: bit(1) -> Types.BIT 可使用setBoolean
            // warn: bit(>1) -> Types.VARBINARY 可使用setBytes
            case Types.BIT:
                if (this.dataBaseType == DataBaseType.MySql) {
                    preparedStatement.setBoolean(columnIndex + 1, column.asBoolean());
                } else {
                    preparedStatement.setString(columnIndex + 1, column.asString());
                }
                break;
            default:
                throw DataXException
                        .asDataXException(
                                DBUtilErrorCode.UNSUPPORTED_TYPE,
                                String.format(
                                        "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%d], 字段Java类型:[%s]. 请修改表中该字段的类型或者不同步该字段.",
                                        this.resultSetMetaData.getLeft()
                                                .get(columnIndex),
                                        this.resultSetMetaData.getMiddle()
                                                .get(columnIndex),
                                        this.resultSetMetaData.getRight()
                                                .get(columnIndex)));
        }
        return preparedStatement;
    }
}
