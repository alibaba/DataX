package cn.hashdata.datax.plugin.writer.gpdbwriter;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.util.WriterUtil;

public class CopyWorker implements Callable<Long> {
	private static final Logger LOG = LoggerFactory.getLogger(CopyWorker.class);

	private CopyWriterTask task = null;
	private Connection connection;
	private LinkedBlockingQueue<byte[]> queue = null;
	private FutureTask<Long> copyResult = null;
	private String sql = null;
	private PipedInputStream pipeIn = null;
	private PipedOutputStream pipeOut = null;
	private Thread copyBackendThread = null;

	public CopyWorker(CopyWriterTask task, String copySql, LinkedBlockingQueue<byte[]> queue) throws IOException {
		this.task = task;
		this.connection = task.createConnection();
		this.queue = queue;
		this.pipeOut = new PipedOutputStream();
		this.pipeIn = new PipedInputStream(pipeOut);
		this.sql = copySql;

		if (task.getMaxCsvLineSize() >= 1024) {
			changeCsvSizelimit(connection);
		}

		this.copyResult = new FutureTask<Long>(new Callable<Long>() {

			@Override
			public Long call() throws Exception {
				try {
					CopyManager mgr = new CopyManager((BaseConnection) connection);
					return mgr.copyIn(sql, pipeIn);
				} finally {
					try {
						pipeIn.close();
					} catch (Exception ignore) {
					}
				}
			}
		});

		copyBackendThread = new Thread(copyResult);
		copyBackendThread.setName(sql);
		copyBackendThread.setDaemon(true);
		copyBackendThread.start();
	}

	@Override
	public Long call() throws Exception {
		Thread.currentThread().setName("CopyWorker");

		byte[] data = null;
		try {
			while (true) {
				data = queue.poll(1000L, TimeUnit.MILLISECONDS);

				if (data == null && false == task.moreData()) {
					break;
				} else if (data == null) {
					continue;
				}

				pipeOut.write(data);
			}

			pipeOut.flush();
			pipeOut.close();
		} catch (Exception e) {
			try {
				((BaseConnection) connection).cancelQuery();
			} catch (SQLException ignore) {
				// ignore if failed to cancel query
			}

			try {
				copyBackendThread.interrupt();
			} catch (SecurityException ignore) {
			}

			try {
				copyResult.get();
			} catch (ExecutionException exec) {
				if (exec.getCause() instanceof PSQLException) {
					throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, exec.getCause());
				}
				// ignore others
			} catch (Exception ignore) {
			}

			throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
		} finally {
			try {
				pipeOut.close();
			} catch (Exception e) {
				// ignore if failed to close pipe
			}

			try {
				copyBackendThread.join(0);
			} catch (Exception e) {
				// ignore if thread is interrupted
			}

			DBUtil.closeDBResources(null, null, connection);
		}

		try {
			Long count = copyResult.get();
			return count;
		} catch (Exception e) {
			throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
		}
	}

	private void changeCsvSizelimit(Connection conn) {
		List<String> sqls = new ArrayList<String>();
		sqls.add("set gp_max_csv_line_length = " + Integer.toString(task.getMaxCsvLineSize()));

		try {
			WriterUtil.executeSqls(conn, sqls, task.getJdbcUrl(), DataBaseType.PostgreSQL);
		} catch (Exception e) {
			LOG.warn("Cannot set gp_max_csv_line_length to " + task.getMaxCsvLineSize());
		}
	}
}
