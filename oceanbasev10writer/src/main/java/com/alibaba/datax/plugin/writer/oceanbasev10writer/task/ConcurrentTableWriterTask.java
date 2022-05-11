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
import com.alibaba.datax.plugin.writer.oceanbasev10writer.ext.ConnHolder;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.ext.ObClientConnHolder;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.ext.ServerConnectInfo;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.util.ObWriterUtils;
import com.alipay.oceanbase.obproxy.data.TableEntryKey;
import com.alipay.oceanbase.obproxy.util.ObPartitionIdCalculator;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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

//import java.sql.PreparedStatement;

public class ConcurrentTableWriterTask extends CommonRdbmsWriter.Task {
    private static final Logger LOG = LoggerFactory.getLogger(ConcurrentTableWriterTask.class);

	// memstore_total 与 memstore_limit 比例的阈值,一旦超过这个值,则暂停写入
	private double memstoreThreshold = Config.DEFAULT_MEMSTORE_THRESHOLD;
	// memstore检查的间隔
	private long memstoreCheckIntervalSecond = Config.DEFAULT_MEMSTORE_CHECK_INTERVAL_SECOND;
	// 最后一次检查
	private long lastCheckMemstoreTime;
    
    private static AtomicLong totalTask = new AtomicLong(0);
    private long taskId = -1;
    
    private AtomicBoolean isMemStoreFull = new AtomicBoolean(false);
    private ConnHolder checkConnHolder;

    public ConcurrentTableWriterTask(DataBaseType dataBaseType) {
		super(dataBaseType);
		taskId = totalTask.getAndIncrement();
	}

	private ObPartitionIdCalculator partCalculator = null;

	private HashMap<Long, List<Record>> groupInsertValues;
	List<Record> unknownPartRecords = new ArrayList<Record>();
//	private List<Record> unknownPartRecords;
	private List<Integer> partitionKeyIndexes;
	
	private ConcurrentTableWriter concurrentWriter = null;
	
	private ConnHolder connHolder;
	
	private boolean allTaskInQueue = false;
	
	private Lock lock = new ReentrantLock();
	private Condition condition = lock.newCondition();
	
	private long startTime;
	private boolean isOb2 = false;
	private String obWriteMode = "update";
	private boolean isOracleCompatibleMode = false;
	private String obUpdateColumns = null;
	private List<Pair<String, int[]>> deleteColPos;
	private String dbName;

	@Override
	public void init(Configuration config) {
		super.init(config);
		// OceanBase 所有操作都是 insert into on duplicate key update 模式
		// writeMode应该使用enum来定义
		this.writeMode = "update";
        obWriteMode = config.getString(Config.OB_WRITE_MODE, "update");
		ServerConnectInfo connectInfo = new ServerConnectInfo(jdbcUrl, username, password);
		dbName = connectInfo.databaseName;
		//init check memstore
		this.memstoreThreshold = config.getDouble(Config.MEMSTORE_THRESHOLD, Config.DEFAULT_MEMSTORE_THRESHOLD);
		this.memstoreCheckIntervalSecond = config.getLong(Config.MEMSTORE_CHECK_INTERVAL_SECOND,
				Config.DEFAULT_MEMSTORE_CHECK_INTERVAL_SECOND);
        this.isOracleCompatibleMode = ObWriterUtils.isOracleMode();

        LOG.info("configure url is unavailable, use obclient for connections.");
        this.checkConnHolder = new ObClientConnHolder(config, connectInfo.jdbcUrl,
				connectInfo.getFullUserName(), connectInfo.password);
        this.connHolder = new ObClientConnHolder(config, connectInfo.jdbcUrl,
				connectInfo.getFullUserName(), connectInfo.password);
        checkConnHolder.initConnection();
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
            initPartCalculator(connectInfo);
        } else {
            LOG.info("Disable partition calculation feature.");
        }

		obUpdateColumns = config.getString(Config.OB_UPDATE_COLUMNS, null);
		groupInsertValues = new HashMap<Long, List<Record>>();
		partitionKeyIndexes = new ArrayList<Integer>();
		rewriteSql();

		if (null == concurrentWriter) {
			concurrentWriter = new ConcurrentTableWriter(config, connectInfo, writeRecordSql);
			allTaskInQueue = false;
		}

		String version = config.getString(Config.OB_VERSION);
		int pIdx = version.lastIndexOf('.');
		if ((Float.valueOf(version.substring(0, pIdx)) >= 2.1f)) {
			isOb2 = true;
		}
	}

	private void initPartCalculator(ServerConnectInfo connectInfo) {
		int retry = 0;
		LOG.info(String.format("create tableEntryKey with clusterName %s, tenantName %s, databaseName %s, tableName %s",
				connectInfo.clusterName, connectInfo.tenantName, connectInfo.databaseName, table));
		TableEntryKey tableEntryKey = new TableEntryKey(connectInfo.clusterName, connectInfo.tenantName,
				connectInfo.databaseName, table);
		do {
			try {
				if (retry > 0) {
					int sleep = retry > 8 ? 500 : (1 << retry);
					TimeUnit.SECONDS.sleep(sleep);
					LOG.info("retry create new part calculator, the {} times", retry);
				}
				LOG.info("create partCalculator with address: " + connectInfo.ipPort);
				partCalculator = new ObPartitionIdCalculator(connectInfo.ipPort, tableEntryKey);
			} catch (Exception ex) {
				++retry;
				LOG.warn("create new part calculator failed, retry {}: {}", retry, ex.getMessage());
			}
		} while (partCalculator == null && retry < 3); // try 3 times
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
			deleteColPos = ObWriterUtils.buildDeleteSql(conn, dbName, table, columns);
		}
		this.writeRecordSql = ObWriterUtils.buildWriteSql(table, columns, conn, obWriteMode, obUpdateColumns);
		LOG.info("writeRecordSql :{}", this.writeRecordSql);
	}
	
	public void prepare(Configuration writerSliceConfig) {
		super.prepare(writerSliceConfig);
		calPartitionKeyIndex(partitionKeyIndexes);
		concurrentWriter.start();
	}

	private void calPartitionKeyIndex(List<Integer> partKeyIndexes) {
		partKeyIndexes.clear();
		if (null == partCalculator) {
			LOG.error("partCalculator is null");
			return;
		}
		for (int i = 0; i < columns.size(); ++i) {
			if (partCalculator.isPartitionKeyColumn(columns.get(i))) {
			    LOG.info(columns.get(i) + " is partition key.");
				partKeyIndexes.add(i);
			}
		}
	}

	private Long calPartitionId(List<Integer> partKeyIndexes, Record record) {
	    if (partCalculator == null) {
	        return null;
	    }
		for (Integer i : partKeyIndexes) {
			partCalculator.addColumn(columns.get(i), record.getColumn(i).asString());
		}
		return partCalculator.calculate();
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

    public PreparedStatement fillStatementIndex(PreparedStatement preparedStatement,
                                                int prepIdx, int columnIndex, Column column) throws SQLException {
        int columnSqltype = this.resultSetMetaData.getMiddle().get(columnIndex);
        String typeName = this.resultSetMetaData.getRight().get(columnIndex);
        return fillPreparedStatementColumnType(preparedStatement, prepIdx, columnSqltype, typeName, column);
    }

    public void collectDirtyRecord(Record record, SQLException e) {
		taskPluginCollector.collectDirtyRecord(record, e);
	}

	public void insertOneRecord(Connection connection, List<Record> buffer) {
		doOneInsert(connection, buffer);
	}

	private void addLeftRecords() {
		//不需要刷新Cache，已经是最后一批数据了
		for (List<Record> groupValues : groupInsertValues.values()) {
			if (groupValues.size() > 0 ) {
				addRecordsToWriteQueue(groupValues);
			}
		}
		if (unknownPartRecords.size() > 0) {
			addRecordsToWriteQueue(unknownPartRecords);
		}
	}
	
	private void addRecordToCache(final Record record) {
		Long partId =null;
		try {
			partId = calPartitionId(partitionKeyIndexes, record);
		} catch (Exception e1) {
		    LOG.warn("fail to get partition id: " + e1.getMessage() + ", record: " + record);
		}

        if (partId == null && isOb2) {
            LOG.debug("fail to calculate parition id, just put into the default buffer.");
            partId = Long.MAX_VALUE;
        }

		if (partId != null) {
			List<Record> groupValues = groupInsertValues.get(partId);
			if (groupValues == null) {
				groupValues = new ArrayList<Record>(batchSize);
				groupInsertValues.put(partId, groupValues);
			}
			groupValues.add(record);
			if (groupValues.size() >= batchSize) {
				groupValues = addRecordsToWriteQueue(groupValues);
				groupInsertValues.put(partId, groupValues);
			}
		} else {
			LOG.debug("add unknown part record {}", record);
			unknownPartRecords.add(record);
			if (unknownPartRecords.size() >= batchSize) {
				unknownPartRecords = addRecordsToWriteQueue(unknownPartRecords);
			}

		}
	}

	/**
	 *
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
		Connection checkConn = checkConnHolder.reconnect();
		long now = System.currentTimeMillis();
		if (now - lastCheckMemstoreTime < 1000 * memstoreCheckIntervalSecond) {
			return;
		}
		boolean isFull = ObWriterUtils.isMemstoreFull(checkConn, memstoreThreshold);
		this.isMemStoreFull.set(isFull);
		if (isFull) {
			LOG.warn("OB memstore is full,sleep 30 seconds, threshold=" + memstoreThreshold);
		}
		lastCheckMemstoreTime = now;
	}
	
	public boolean isMemStoreFull() {
		return isMemStoreFull.get();
	}
	
	public void printEveryTime() {
		long cost = System.currentTimeMillis() - startTime;
		if (cost > 10000) { //10s
			print();
			startTime = System.currentTimeMillis();
		}
	}
	
	public void print() {
		LOG.debug("Statistic total task {}, finished {}, queue Size {}",
				concurrentWriter.getTotalTaskCount(),
				concurrentWriter.getFinishTaskCount(),
				concurrentWriter.getTaskQueueSize());
		concurrentWriter.printStatistics();
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
	   if(concurrentWriter!=null) {
		concurrentWriter.destory();
		}
		// 把本级持有的conn关闭掉
		DBUtil.closeDBResources(null, connHolder.getConn());
        DBUtil.closeDBResources(null, checkConnHolder.getConn());
        checkConnHolder.destroy();
		super.destroy(writerSliceConfig);
	}
	
	public class ConcurrentTableWriter {
		private BlockingQueue<List<Record>> queue;
		private List<InsertTask> insertTasks;
		private Configuration config;
		private ServerConnectInfo connectInfo;
		private String rewriteRecordSql;
		private AtomicLong totalTaskCount;
		private AtomicLong finishTaskCount;
		private final int threadCount;

		public ConcurrentTableWriter(Configuration config, ServerConnectInfo connInfo, String rewriteRecordSql) {
			threadCount = config.getInt(Config.WRITER_THREAD_COUNT, Config.DEFAULT_WRITER_THREAD_COUNT);
			queue = new LinkedBlockingQueue<List<Record>>(threadCount << 1);
			insertTasks = new ArrayList<InsertTask>(threadCount);
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
			    LOG.info("start {} insert task.", (i+1));
				InsertTask insertTask = new InsertTask(taskId, queue, config, connectInfo, rewriteRecordSql, deleteColPos);
				insertTask.setWriterTask(ConcurrentTableWriterTask.this);
				insertTask.setWriter(this);
				insertTasks.add(insertTask);
			}
			WriterThreadPool.executeBatch(insertTasks);
		}
		
		public void printStatistics() {
			long insertTotalCost = 0;
			long insertTotalCount = 0;
			for (InsertTask task: insertTasks) {
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
				isSucc = queue.offer(records, 5, TimeUnit.SECONDS);
				checkMemStore();
			}
			totalTaskCount.incrementAndGet();
		}
		
		public synchronized void destory() {
			if (insertTasks != null) {
				for(InsertTask task : insertTasks) {
					task.setStop();
				}
				for(InsertTask task: insertTasks) {
					task.destroy();
				}
			}
		}
	}
}
