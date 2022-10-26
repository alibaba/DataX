package cn.hashdata.datax.plugin.writer.gpdbwriter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;

public class CopyWriterTask extends CommonRdbmsWriter.Task {
	private static final Logger LOG = LoggerFactory.getLogger(CopyWriterTask.class);
	private Configuration writerSliceConfig = null;
	private int numProcessor;
	private int maxCsvLineSize;
	private int numWriter;
	private int queueSize;
	private volatile boolean stopProcessor = false;
	private volatile boolean stopWriter = false;

	private CompletionService<Long> cs = null;

	public CopyWriterTask() {
		super(DataBaseType.PostgreSQL);
	}

	public String getJdbcUrl() {
		return this.jdbcUrl;
	}

	public Connection createConnection() {
		Connection connection = DBUtil.getConnection(this.dataBaseType, this.jdbcUrl, username, password);
		DBUtil.dealWithSessionConfig(connection, writerSliceConfig, this.dataBaseType, BASIC_MESSAGE);
		return connection;
	}

	private String constructColumnNameList(List<String> columnList) {
		List<String> columns = new ArrayList<String>();

		for (String column : columnList) {
			if (column.endsWith("\"") && column.startsWith("\"")) {
				columns.add(column);
			} else {
				columns.add("\"" + column + "\"");
			}
		}

		return StringUtils.join(columns, ",");
	}

	public String getCopySql(String tableName, List<String> columnList, int segment_reject_limit) {
		StringBuilder sb = new StringBuilder().append("COPY ").append(tableName).append("(")
				.append(constructColumnNameList(columnList))
				.append(") FROM STDIN WITH DELIMITER '|' NULL '' CSV QUOTE '\"' ESCAPE E'\\\\'");

		if (segment_reject_limit >= 2) {
			sb.append(" LOG ERRORS SEGMENT REJECT LIMIT ").append(segment_reject_limit).append(";");
		} else {
			sb.append(";");
		}

		String sql = sb.toString();
		return sql;
	}

	private void send(Record record, LinkedBlockingQueue<Record> queue)
			throws InterruptedException, ExecutionException {
		while (queue.offer(record, 1000, TimeUnit.MILLISECONDS) == false) {
			LOG.debug("Record queue is full, increase num_copy_processor for performance.");
			Future<Long> result = cs.poll();

			if (result != null) {
				result.get();
			}
		}
	}

	public boolean moreRecord() {
		return !stopProcessor;
	}

	public boolean moreData() {
		return !stopWriter;
	}

	public int getMaxCsvLineSize() {
		return maxCsvLineSize;
	}

	@Override
	public void startWrite(RecordReceiver recordReceiver, Configuration writerSliceConfig,
			TaskPluginCollector taskPluginCollector) {
		this.writerSliceConfig = writerSliceConfig;
		int segment_reject_limit = writerSliceConfig.getInt("segment_reject_limit", 0);
		this.queueSize = writerSliceConfig.getInt("copy_queue_size", 1000);
		this.queueSize = Math.max(this.queueSize, 10);
		this.numProcessor = writerSliceConfig.getInt("num_copy_processor", 4);
		this.numProcessor = Math.max(this.numProcessor, 1);
		this.numWriter = writerSliceConfig.getInt("num_copy_writer", 1);
		this.numWriter = Math.max(this.numWriter, 1);
		this.maxCsvLineSize = writerSliceConfig.getInt("max_csv_line_size", 0);

		String sql = getCopySql(this.table, this.columns, segment_reject_limit);
		LinkedBlockingQueue<Record> recordQueue = new LinkedBlockingQueue<Record>(queueSize);
		LinkedBlockingQueue<byte[]> dataQueue = new LinkedBlockingQueue<byte[]>(queueSize);
		ExecutorService threadPool;

		threadPool = Executors.newFixedThreadPool(this.numProcessor + this.numWriter);
		cs = new ExecutorCompletionService<Long>(threadPool);
		Connection connection = createConnection();

		try {

			this.resultSetMetaData = DBUtil.getColumnMetaData(connection, this.table,
					constructColumnNameList(this.columns));
			for (int i = 0; i < numProcessor; i++) {
				cs.submit(new CopyProcessor(this, this.columnNumber, resultSetMetaData, recordQueue, dataQueue));
			}

			for (int i = 0; i < numWriter; i++) {
				cs.submit(new CopyWorker(this, sql, dataQueue));
			}

			Record record;
			while ((record = recordReceiver.getFromReader()) != null) {
				send(record, recordQueue);
				Future<Long> result = cs.poll();

				if (result != null) {
					result.get();
				}
			}

			stopProcessor = true;
			for (int i = 0; i < numProcessor; i++) {
				cs.take().get();
			}

			stopWriter = true;
			for (int i = 0; i < numWriter; i++) {
				cs.take().get();
			}
		} catch (ExecutionException e) {
			throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e.getCause());
		} catch (Exception e) {
			throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
		} finally {
			threadPool.shutdownNow();
			DBUtil.closeDBResources(null, null, connection);
		}
	}
}
