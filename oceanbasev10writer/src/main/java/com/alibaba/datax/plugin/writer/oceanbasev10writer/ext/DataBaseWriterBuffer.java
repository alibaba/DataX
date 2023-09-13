package com.alibaba.datax.plugin.writer.oceanbasev10writer.ext;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.util.ObWriterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author oceanbase
 *
 */
public class DataBaseWriterBuffer {
	private static final Logger LOG = LoggerFactory.getLogger(DataBaseWriterBuffer.class);

	private final AbstractConnHolder connHolder;
	private final String dbName;
	private Map<String, LinkedList<Record>> tableBuffer = new HashMap<String, LinkedList<Record>>();
	private long lastCheckMemstoreTime;

	public DataBaseWriterBuffer(Configuration config,String jdbcUrl, String userName, String password,String dbName){
		this.connHolder = new ObClientConnHolder(config, jdbcUrl, userName, password);
		this.dbName=dbName;
	}
	
	public AbstractConnHolder getConnHolder(){
		return connHolder;
	}

	public void initTableBuffer(List<String> tableList) {
		for (String table : tableList) {
			tableBuffer.put(table, new LinkedList<Record>());
		}
	}
	
	public List<String> getTableList(){
		return new ArrayList<String>(tableBuffer.keySet());
	}

	public void addRecord(Record record, String tableName) {
		LinkedList<Record> recordList = tableBuffer.get(tableName);
		if (recordList == null) {
			throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR,
					String.format("The [table] calculated based on the rules does not exist. The calculated [tableName]=%s, [db]=%s. Please check the rules you configured.",
							tableName, connHolder.getJdbcUrl()));
		}
		recordList.add(record);
	}

	public Map<String, LinkedList<Record>> getTableBuffer() {
		return tableBuffer;
	}

	public String getDbName() {
		return dbName;
	}

	public long getLastCheckMemstoreTime() {
		return lastCheckMemstoreTime;
	}

	public void setLastCheckMemstoreTime(long lastCheckMemstoreTime) {
		this.lastCheckMemstoreTime = lastCheckMemstoreTime;
	}

	/**
	 * 检查当前DB的memstore使用状态
	 * <p>
	 * 若超过阈值,则休眠
	 *
	 * @param memstoreCheckIntervalSecond
	 * @param memstoreThreshold
	 */
	public synchronized void checkMemstore(long memstoreCheckIntervalSecond, double memstoreThreshold) {
		long now = System.currentTimeMillis();
		if (now - getLastCheckMemstoreTime() < 1000 * memstoreCheckIntervalSecond) {
			return;
		}

		LOG.debug(String.format("checking memstore usage: lastCheckTime=%d, now=%d, check interval=%d, threshold=%f",
				getLastCheckMemstoreTime(), now, memstoreCheckIntervalSecond, memstoreThreshold));

		Connection conn = getConnHolder().getConn();
		while (ObWriterUtils.isMemstoreFull(conn, memstoreThreshold)) {
			LOG.warn("OB memstore is full,sleep 60 seconds, jdbc=" + getConnHolder().getJdbcUrl()
					+ ",threshold=" + memstoreThreshold);
			ObWriterUtils.sleep(60000);
		}
		setLastCheckMemstoreTime(now);
	}
}
