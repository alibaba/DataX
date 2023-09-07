package com.alibaba.datax.plugin.writer.oceanbasev10writer;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.datax.plugin.rdbms.writer.util.WriterUtil;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.task.ConcurrentTableWriterTask;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.util.DbUtils;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.util.ObWriterUtils;
import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * 2016-04-07
 * <p>
 * 专门针对OceanBase1.0的Writer
 * 
 * @author biliang.wbl
 *
 */
public class OceanBaseV10Writer extends Writer {
	private static DataBaseType DATABASE_TYPE = DataBaseType.OceanBase;

	/**
	 * Job 中的方法仅执行一次，Task 中方法会由框架启动多个 Task 线程并行执行。
	 * <p/>
	 * 整个 Writer 执行流程是：
	 * 
	 * <pre>
	 * Job类init-->prepare-->split
	 * 
	 *                          Task类init-->prepare-->startWrite-->post-->destroy
	 *                          Task类init-->prepare-->startWrite-->post-->destroy
	 * 
	 *                                                                            Job类post-->destroy
	 * </pre>
	 */
	public static class Job extends Writer.Job {
		private Configuration originalConfig = null;
		private CommonRdbmsWriter.Job commonJob;
		private static final Logger LOG = LoggerFactory.getLogger(Job.class);

		/**
		 * 注意：此方法仅执行一次。 最佳实践：通常在这里对用户的配置进行校验：是否缺失必填项？有无错误值？有没有无关配置项？...
		 * 并给出清晰的报错/警告提示。校验通常建议采用静态工具类进行，以保证本类结构清晰。
		 */
		@Override
		public void init() {
			this.originalConfig = super.getPluginJobConf();
			checkCompatibleMode(originalConfig);
			//将config中的column和table中的关键字进行转义
			List<String> columns = originalConfig.getList(Key.COLUMN, String.class);
			ObWriterUtils.escapeDatabaseKeyword(columns);
			originalConfig.set(Key.COLUMN, columns);

			List<JSONObject> conns = originalConfig.getList(Constant.CONN_MARK, JSONObject.class);
			for (int i = 0; i < conns.size(); i++) {
				JSONObject conn = conns.get(i);
				Configuration connConfig = Configuration.from(conn.toString());
				List<String> tables = connConfig.getList(Key.TABLE, String.class);
				ObWriterUtils.escapeDatabaseKeyword(tables);
				originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, i, Key.TABLE), tables);
			}
			this.commonJob = new CommonRdbmsWriter.Job(DATABASE_TYPE);
			this.commonJob.init(this.originalConfig);
		}

		/**
		 * 注意：此方法仅执行一次。 最佳实践：如果 Job 中有需要进行数据同步之前的处理，可以在此处完成，如果没有必要则可以直接去掉。
		 */
		// 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
		@Override
		public void prepare() {
			int tableNumber = originalConfig.getInt(Constant.TABLE_NUMBER_MARK);
			if (tableNumber == 1) {
				this.commonJob.prepare(this.originalConfig);
				final String version = fetchServerVersion(originalConfig);
				ObWriterUtils.setObVersion(version);
				originalConfig.set(Config.OB_VERSION, version);
			}

			String username = originalConfig.getString(Key.USERNAME);
			String password = originalConfig.getString(Key.PASSWORD);

			// 获取presql配置，并执行
			List<String> preSqls = originalConfig.getList(Key.PRE_SQL, String.class);
			if (preSqls == null || preSqls.size() == 0) {
				return;
			}

			List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
			for (Object connConfObject : conns) {
				Configuration connConf = Configuration.from(connConfObject.toString());
				// 这里的 jdbcUrl 已经 append 了合适后缀参数
				String jdbcUrl = connConf.getString(Key.JDBC_URL);

				List<String> tableList = connConf.getList(Key.TABLE, String.class);
				for (String table : tableList) {
					List<String> renderedPreSqls = WriterUtil.renderPreOrPostSqls(preSqls, table);
					if (null != renderedPreSqls && !renderedPreSqls.isEmpty()) {
						Connection conn = DBUtil.getConnection(DATABASE_TYPE, jdbcUrl, username, password);
						LOG.info("Begin to execute preSqls:[{}]. context info:{}.",
								StringUtils.join(renderedPreSqls, ";"), jdbcUrl);
						WriterUtil.executeSqls(conn, renderedPreSqls, jdbcUrl, DATABASE_TYPE);
						ObWriterUtils.asyncClose(null, null, conn);
					}
				}
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug("After job prepare(), originalConfig now is:[\n{}\n]", originalConfig.toJSON());
			}
		}

		/**
		 * 注意：此方法仅执行一次。 最佳实践：通常采用工具静态类完成把 Job 配置切分成多个 Task 配置的工作。 这里的
		 * mandatoryNumber 是强制必须切分的份数。
		 */
		@Override
		public List<Configuration> split(int mandatoryNumber) {
			int tableNumber = originalConfig.getInt(Constant.TABLE_NUMBER_MARK);
			if (tableNumber == 1) {
				return this.commonJob.split(this.originalConfig, mandatoryNumber);
			}
			Configuration simplifiedConf = this.originalConfig;

			List<Configuration> splitResultConfigs = new ArrayList<Configuration>();
			for (int j = 0; j < mandatoryNumber; j++) {
				splitResultConfigs.add(simplifiedConf.clone());
			}
			return splitResultConfigs;
		}

		/**
		 * 注意：此方法仅执行一次。 最佳实践：如果 Job 中有需要进行数据同步之后的后续处理，可以在此处完成。
		 */
		@Override
		public void post() {
			int tableNumber = originalConfig.getInt(Constant.TABLE_NUMBER_MARK);
			if (tableNumber == 1) {
				commonJob.post(this.originalConfig);
				return;
			}
			String username = originalConfig.getString(Key.USERNAME);
			String password = originalConfig.getString(Key.PASSWORD);
			List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
			List<String> postSqls = originalConfig.getList(Key.POST_SQL, String.class);
			if (postSqls == null || postSqls.size() == 0) {
				return;
			}

			for (Object connConfObject : conns) {
				Configuration connConf = Configuration.from(connConfObject.toString());
				String jdbcUrl = connConf.getString(Key.JDBC_URL);
				List<String> tableList = connConf.getList(Key.TABLE, String.class);

				for (String table : tableList) {
					List<String> renderedPostSqls = WriterUtil.renderPreOrPostSqls(postSqls, table);
					if (null != renderedPostSqls && !renderedPostSqls.isEmpty()) {
						// 说明有 postSql 配置，则此处删除掉
						Connection conn = DBUtil.getConnection(DATABASE_TYPE, jdbcUrl, username, password);
						LOG.info("Begin to execute postSqls:[{}]. context info:{}.",
								StringUtils.join(renderedPostSqls, ";"), jdbcUrl);
						WriterUtil.executeSqls(conn, renderedPostSqls, jdbcUrl, DATABASE_TYPE);
						ObWriterUtils.asyncClose(null, null, conn);
					}
				}
			}
			originalConfig.remove(Key.POST_SQL);
		}
		
		/**
		 * 注意：此方法仅执行一次。 最佳实践：通常配合 Job 中的 post() 方法一起完成 Job 的资源释放。
		 */
		@Override
		public void destroy() {
			this.commonJob.destroy(this.originalConfig);
		}

		private String fetchServerVersion(Configuration config) {
			final String fetchVersionSql = "show variables like 'version_comment'";
			String versionComment =  DbUtils.fetchSingleValueWithRetry(config, fetchVersionSql);
			return versionComment.split(" ")[1];
		}

		private void checkCompatibleMode(Configuration configure) {
			final String fetchCompatibleModeSql = "SHOW VARIABLES LIKE 'ob_compatibility_mode'";
			String compatibleMode = DbUtils.fetchSingleValueWithRetry(configure, fetchCompatibleModeSql);
			ObWriterUtils.setCompatibleMode(compatibleMode);
			configure.set(Config.OB_COMPATIBLE_MODE, compatibleMode);
		}
	}

	public static class Task extends Writer.Task {
		private static final Logger LOG = LoggerFactory.getLogger(Task.class);
		private Configuration writerSliceConfig;
		private CommonRdbmsWriter.Task writerTask;

		/**
		 * 注意：此方法每个 Task 都会执行一次。 最佳实践：此处通过对 taskConfig 配置的读取，进而初始化一些资源为
		 * startWrite()做准备。
		 */
		@Override
		public void init() {
			this.writerSliceConfig = super.getPluginJobConf();
			int tableNumber = writerSliceConfig.getInt(Constant.TABLE_NUMBER_MARK);
			if (tableNumber == 1) {
				// always use concurrentTableWriter
				this.writerTask = new ConcurrentTableWriterTask(DATABASE_TYPE);
			} else {
				throw new RuntimeException("writing to multi-tables is not supported.");
			}
			LOG.info("tableNumber:" + tableNumber + ",writerTask Class:" + writerTask.getClass().getName());
			this.writerTask.init(this.writerSliceConfig);
		}

		/**
		 * 注意：此方法每个 Task 都会执行一次。 最佳实践：如果 Task
		 * 中有需要进行数据同步之前的处理，可以在此处完成，如果没有必要则可以直接去掉。
		 */
		@Override
		public void prepare() {
			this.writerTask.prepare(this.writerSliceConfig);
		}

		/**
		 * 注意：此方法每个 Task 都会执行一次。 最佳实践：此处适当封装确保简洁清晰完成数据写入工作。
		 */
		@Override
		public void startWrite(RecordReceiver recordReceiver) {
			this.writerTask.startWrite(recordReceiver, this.writerSliceConfig, super.getTaskPluginCollector());
		}

		/**
		 * 注意：此方法每个 Task 都会执行一次。 最佳实践：如果 Task 中有需要进行数据同步之后的后续处理，可以在此处完成。
		 */
		@Override
		public void post() {
			this.writerTask.post(this.writerSliceConfig);
		}

		/**
		 * 注意：此方法每个 Task 都会执行一次。 最佳实践：通常配合Task 中的 post() 方法一起完成 Task 的资源释放。
		 */
		@Override
		public void destroy() {
			this.writerTask.destroy(this.writerSliceConfig);
		}
	}
}
