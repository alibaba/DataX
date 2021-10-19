package com.alibaba.datax.plugin.writer.oceanbasev10writer.task;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.ext.ObClientConnHolder;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.Config;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.ext.ConnHolder;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.ext.ServerConnectInfo;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.task.ConcurrentTableWriterTask.ConcurrentTableWriter;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.util.ObWriterUtils;

public class InsertTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(InsertTask.class);

	private ConcurrentTableWriterTask writerTask;
	private ConcurrentTableWriter writer;

	private String writeRecordSql;
	private long totalCost = 0;
	private long insertCount = 0;

	private Queue<List<Record>> queue;
	private boolean isStop;
	private ConnHolder connHolder;

	private final long taskId;
	private ServerConnectInfo connInfo;

	// 失败重试次数
	private int failTryCount = Config.DEFAULT_FAIL_TRY_COUNT;
	private boolean printCost = Config.DEFAULT_PRINT_COST;
	private long costBound = Config.DEFAULT_COST_BOUND;
	private List<Pair<String, int[]>> deleteMeta;

	public InsertTask(
			final long taskId,
			Queue<List<Record>> recordsQueue,
			Configuration config,
			ServerConnectInfo connectInfo,
			String writeRecordSql,
			List<Pair<String, int[]>> deleteMeta) {
		this.taskId = taskId;
		this.queue = recordsQueue;
		this.connInfo = connectInfo;
		failTryCount = config.getInt(Config.FAIL_TRY_COUNT, Config.DEFAULT_FAIL_TRY_COUNT);
		printCost = config.getBool(Config.PRINT_COST, Config.DEFAULT_PRINT_COST);
		costBound = config.getLong(Config.COST_BOUND, Config.DEFAULT_COST_BOUND);
		this.connHolder = new ObClientConnHolder(config, connInfo.jdbcUrl,
                                         connInfo.getFullUserName(), connInfo.password);
		this.writeRecordSql = writeRecordSql;
		this.isStop = false;
		this.deleteMeta = deleteMeta;
		connHolder.initConnection();
	}
	
	void setWriterTask(ConcurrentTableWriterTask writerTask) {
		this.writerTask = writerTask;
	}
	
	void setWriter(ConcurrentTableWriter writer) {
		this.writer = writer;
	}

	private boolean isStop() { return isStop; }
	public void setStop() { isStop = true; }
	public long getTotalCost() { return totalCost; }
	public long getInsertCount() { return insertCount; }
	
	@Override
	public void run() {
		Thread.currentThread().setName(String.format("%d-insertTask-%d", taskId, Thread.currentThread().getId()));
		LOG.debug("Task {} start to execute...", taskId);
		while (!isStop()) {
			try {
				List<Record> records = queue.poll();
				if (null != records) {
					doMultiInsert(records, this.printCost, this.costBound);

				} else if (writerTask.isFinished()) {
					writerTask.singalTaskFinish();
					LOG.debug("not more task, thread exist ...");
					break;
				} else {
					TimeUnit.MILLISECONDS.sleep(5);
				}
			} catch (InterruptedException e) {
				LOG.debug("TableWriter is interrupt");
			} catch (Exception e) {
				LOG.warn("ERROR UNEXPECTED {}", e);
			}
		}
		LOG.debug("Thread exist...");
	}
	
	public void destroy() {
	    connHolder.destroy();
	};
	
	public void calStatistic(final long cost) {
		writer.increFinishCount();
		++insertCount;
		totalCost += cost;
		if (this.printCost && cost > this.costBound) {
			LOG.info("slow multi insert cost {}ms", cost);
		}
	}

	private void doDelete(Connection conn, final List<Record> buffer) throws SQLException {
		if(deleteMeta == null || deleteMeta.size() == 0) {
			return;
		}
		for (int i = 0; i < deleteMeta.size(); i++) {
			String deleteSql = deleteMeta.get(i).getKey();
			int[] valueIdx = deleteMeta.get(i).getValue();
			PreparedStatement ps = null;
			try {
				ps = conn.prepareStatement(deleteSql);
				StringBuilder builder = new StringBuilder();
				for (Record record : buffer) {
					int bindIndex = 0;
					for (int idx : valueIdx) {
						writerTask.fillStatementIndex(ps, bindIndex++, idx, record.getColumn(idx));
						builder.append(record.getColumn(idx).asString()).append(",");
					}
					ps.addBatch();
				}
				LOG.debug("delete values: " + builder.toString());
				ps.executeBatch();
			} catch (SQLException ex) {
				LOG.error("SQL Exception when delete records with {}", deleteSql, ex);
				throw ex;
			} finally {
				DBUtil.closeDBResources(ps, null);
			}
		}
	}

	public void doMultiInsert(final List<Record> buffer, final boolean printCost, final long restrict) {
		checkMemstore();
		Connection conn = connHolder.getConn();
		boolean success = false;
		long cost = 0;
		long startTime = 0;
		try {
			for (int i = 0; i < failTryCount; ++i) {
				if (i > 0) {
					try {
						int sleep = i >= 9 ? 500 : 1 << i;//不明白为什么要sleep 500s
						TimeUnit.SECONDS.sleep(sleep);
					} catch (InterruptedException e) {
						LOG.info("thread interrupted ..., ignore");
					}
					conn = connHolder.getConn();
					LOG.info("retry {}, start do batch insert, size={}", i, buffer.size());
					checkMemstore();
				}
				startTime = System.currentTimeMillis();
				PreparedStatement ps = null;
				try {
					conn.setAutoCommit(false);

					// do delete if necessary
					doDelete(conn, buffer);

					ps = conn.prepareStatement(writeRecordSql);
					for (Record record : buffer) {
						ps = writerTask.fillStatement(ps, record);
						ps.addBatch();
					}
					ps.executeBatch();
					conn.commit();
					success = true;
					cost = System.currentTimeMillis() - startTime;
					calStatistic(cost);
					break;
				} catch (SQLException e) {
					LOG.warn("Insert fatal error SqlState ={}, errorCode = {}, {}", e.getSQLState(), e.getErrorCode(), e);
					if (i == 0 || i > 10 ) {
						for (Record record : buffer) {
							LOG.warn("ERROR : record {}", record);
						}
					}
					// 按照错误码分类，分情况处理
					// 如果是OB系统级异常,则需要重建连接
					boolean fatalFail = ObWriterUtils.isFatalError(e);
					if (fatalFail) {
						ObWriterUtils.sleep(300000);
						connHolder.reconnect();
						// 如果是可恢复的异常,则重试
					} else if (ObWriterUtils.isRecoverableError(e)) {
						conn.rollback();
						ObWriterUtils.sleep(60000);
					} else {// 其它异常直接退出,采用逐条写入方式
						conn.rollback();
						ObWriterUtils.sleep(1000);
						break;
					}
				} catch (Exception e) {
					e.printStackTrace();
					LOG.warn("Insert error unexpected {}", e);
				} finally {
					DBUtil.closeDBResources(ps, null);
				}
			}
		} catch (SQLException e) {
			LOG.warn("ERROR:retry failSql State ={}, errorCode = {}, {}", e.getSQLState(), e.getErrorCode(), e);
		}

		if (!success) {
			try {
				LOG.info("do one insert");
				conn = connHolder.reconnect();
				doOneInsert(conn, buffer);
				cost = System.currentTimeMillis() - startTime;
				calStatistic(cost);
			} finally {
			}
		}
	}

	// process one row, delete before insert
	private void doOneInsert(Connection connection, List<Record> buffer) {
		List<PreparedStatement> deletePstmtList = new ArrayList();
		PreparedStatement preparedStatement = null;
		try {
			connection.setAutoCommit(false);
			if (deleteMeta != null && deleteMeta.size() > 0) {
				for (int i = 0; i < deleteMeta.size(); i++) {
					String deleteSql = deleteMeta.get(i).getKey();
					deletePstmtList.add(connection.prepareStatement(deleteSql));
				}
			}

			preparedStatement = connection.prepareStatement(this.writeRecordSql);
			for (Record record : buffer) {
				try {
					for (int i = 0; i < deletePstmtList.size(); i++) {
						PreparedStatement deleteStmt = deletePstmtList.get(i);
						int[] valueIdx = deleteMeta.get(i).getValue();
						int bindIndex = 0;
						for (int idx : valueIdx) {
							writerTask.fillStatementIndex(deleteStmt, bindIndex++, idx, record.getColumn(idx));
						}
						deleteStmt.execute();
					}
					preparedStatement = writerTask.fillStatement(preparedStatement, record);
					preparedStatement.execute();
					connection.commit();
				} catch (SQLException e) {
					writerTask.collectDirtyRecord(record, e);
				} finally {
					// 此处不应该关闭statement，后续的数据还需要用到
				}
			}
		} catch (Exception e) {
			throw DataXException.asDataXException(
					DBUtilErrorCode.WRITE_DATA_ERROR, e);
		} finally {
			DBUtil.closeDBResources(preparedStatement, null);
			for (PreparedStatement pstmt : deletePstmtList) {
				DBUtil.closeDBResources(pstmt, null);
			}
		}
	}

	private void checkMemstore() {
		while (writerTask.isMemStoreFull()) {
			ObWriterUtils.sleep(30000);
		}
	}
}
