package com.alibaba.datax.plugin.writer.hologresjdbcwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.alibaba.datax.plugin.writer.hologresjdbcwriter.util.OriginalConfPretreatmentUtil;
import com.alibaba.datax.plugin.writer.hologresjdbcwriter.util.WriterUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class BaseWriter {

	public static class Job {
		private DataBaseType dataBaseType;

		private static final Logger LOG = LoggerFactory
				.getLogger(BaseWriter.Job.class);

		public Job(DataBaseType dataBaseType) {
			this.dataBaseType = dataBaseType;
			OriginalConfPretreatmentUtil.DATABASE_TYPE = this.dataBaseType;
		}

		public void init(Configuration originalConfig) {
			OriginalConfPretreatmentUtil.doPretreatment(originalConfig, this.dataBaseType);

			LOG.debug("After job init(), originalConfig now is:[\n{}\n]",
					originalConfig.toJSON());
		}

		// 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
		public void prepare(Configuration originalConfig) {
			int tableNumber = originalConfig.getInt(Constant.TABLE_NUMBER_MARK);
			if (tableNumber == 1) {
				String username = originalConfig.getString(Key.USERNAME);
				String password = originalConfig.getString(Key.PASSWORD);

				List<Object> conns = originalConfig.getList(Constant.CONN_MARK,
						Object.class);
				Configuration connConf = Configuration.from(conns.get(0)
						.toString());

				// 这里的 jdbcUrl 已经 append 了合适后缀参数
				String jdbcUrl = connConf.getString(Key.JDBC_URL);
				originalConfig.set(Key.JDBC_URL, jdbcUrl);

				String table = connConf.getList(Key.TABLE, String.class).get(0);
				originalConfig.set(Key.TABLE, table);

				List<String> preSqls = originalConfig.getList(Key.PRE_SQL,
						String.class);
				List<String> renderedPreSqls = WriterUtil.renderPreOrPostSqls(
						preSqls, table);

				originalConfig.remove(Constant.CONN_MARK);
				if (null != renderedPreSqls && !renderedPreSqls.isEmpty()) {
					// 说明有 preSql 配置，则此处删除掉
					originalConfig.remove(Key.PRE_SQL);

					Connection conn = DBUtil.getConnection(dataBaseType,
							jdbcUrl, username, password);
					LOG.info("Begin to execute preSqls:[{}]. context info:{}.",
							StringUtils.join(renderedPreSqls, ";"), jdbcUrl);

					WriterUtil.executeSqls(conn, renderedPreSqls, jdbcUrl, dataBaseType);
					DBUtil.closeDBResources(null, null, conn);
				}
			}

			LOG.debug("After job prepare(), originalConfig now is:[\n{}\n]",
					originalConfig.toJSON());
		}

		public List<Configuration> split(Configuration originalConfig,
										 int mandatoryNumber) {
			return WriterUtil.doSplit(originalConfig, mandatoryNumber);
		}

		// 一般来说，是需要推迟到 task 中进行post 的执行（单表情况例外）
		public void post(Configuration originalConfig) {
			int tableNumber = originalConfig.getInt(Constant.TABLE_NUMBER_MARK);
			if (tableNumber == 1) {
				String username = originalConfig.getString(Key.USERNAME);
				String password = originalConfig.getString(Key.PASSWORD);

				// 已经由 prepare 进行了appendJDBCSuffix处理
				String jdbcUrl = originalConfig.getString(Key.JDBC_URL);

				String table = originalConfig.getString(Key.TABLE);

				List<String> postSqls = originalConfig.getList(Key.POST_SQL,
						String.class);
				List<String> renderedPostSqls = WriterUtil.renderPreOrPostSqls(
						postSqls, table);

				if (null != renderedPostSqls && !renderedPostSqls.isEmpty()) {
					// 说明有 postSql 配置，则此处删除掉
					originalConfig.remove(Key.POST_SQL);

					Connection conn = DBUtil.getConnection(this.dataBaseType,
							jdbcUrl, username, password);

					LOG.info(
							"Begin to execute postSqls:[{}]. context info:{}.",
							StringUtils.join(renderedPostSqls, ";"), jdbcUrl);
					WriterUtil.executeSqls(conn, renderedPostSqls, jdbcUrl, dataBaseType);
					DBUtil.closeDBResources(null, null, conn);
				}
			}
		}

		public void destroy(Configuration originalConfig) {
		}

	}

	public static class Task {
		protected static final Logger LOG = LoggerFactory
				.getLogger(BaseWriter.Task.class);

		protected DataBaseType dataBaseType;

		protected String username;
		protected String password;
		protected String jdbcUrl;
		protected String table;
		protected List<String> columns;
		protected List<String> primaryKeys;
		protected List<String> arrayTypes;
		protected List<String> preSqls;
		protected List<String> postSqls;
		protected int batchSize;
		protected int batchByteSize;
		protected int columnNumber = 0;
		protected int arrayIndex = 0;
		protected TaskPluginCollector taskPluginCollector;

		// 作为日志显示信息时，需要附带的通用信息。比如信息所对应的数据库连接等信息，针对哪个表做的操作
		protected static String BASIC_MESSAGE;

		protected static String INSERT_OR_REPLACE_TEMPLATE;

		protected String writeRecordSql;
		protected String writeMode;
		protected String arrayDelimiter;
		protected boolean emptyAsNull;
		protected Triple<List<String>, List<Integer>, List<String>> resultSetMetaData;

		public Task(DataBaseType dataBaseType) {
			this.dataBaseType = dataBaseType;
		}

		public void init(Configuration writerSliceConfig) {
			this.username = writerSliceConfig.getString(Key.USERNAME);
			this.password = writerSliceConfig.getString(Key.PASSWORD);
			this.jdbcUrl = writerSliceConfig.getString(Key.JDBC_URL);
			this.table = writerSliceConfig.getString(Key.TABLE);

			this.columns = writerSliceConfig.getList(Key.COLUMN, String.class);
			this.columnNumber = this.columns.size();
			this.primaryKeys = writerSliceConfig.getList(Key.PRIMARY_KEY,String.class);

			this.arrayIndex = 0;
			this.arrayTypes = writerSliceConfig.getList(Key.Array_Type, String.class);
			this.arrayDelimiter = writerSliceConfig.getString(Key.Array_Delimiter);

			this.preSqls = writerSliceConfig.getList(Key.PRE_SQL, String.class);
			this.postSqls = writerSliceConfig.getList(Key.POST_SQL, String.class);
			this.batchSize = writerSliceConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
			this.batchByteSize = writerSliceConfig.getInt(Key.BATCH_BYTE_SIZE, Constant.DEFAULT_BATCH_BYTE_SIZE);

			writeMode = writerSliceConfig.getString(Key.WRITE_MODE, "INSERT");
			emptyAsNull = writerSliceConfig.getBool(Key.EMPTY_AS_NULL, true);
			INSERT_OR_REPLACE_TEMPLATE = writerSliceConfig.getString(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK);
			this.writeRecordSql = String.format(INSERT_OR_REPLACE_TEMPLATE, this.table);

			BASIC_MESSAGE = String.format("jdbcUrl:[%s], table:[%s]",
					this.jdbcUrl, this.table);
		}

		public void prepare(Configuration writerSliceConfig) {
			Connection connection = DBUtil.getConnection(this.dataBaseType,
					this.jdbcUrl, username, password);

			DBUtil.dealWithSessionConfig(connection, writerSliceConfig,
					this.dataBaseType, BASIC_MESSAGE);

			int tableNumber = writerSliceConfig.getInt(
					Constant.TABLE_NUMBER_MARK);
			if (tableNumber != 1) {
				LOG.info("Begin to execute preSqls:[{}]. context info:{}.",
						StringUtils.join(this.preSqls, ";"), BASIC_MESSAGE);
				WriterUtil.executeSqls(connection, this.preSqls, BASIC_MESSAGE, dataBaseType);
			}

			DBUtil.closeDBResources(null, null, connection);
		}

		public void startWriteWithConnection(RecordReceiver recordReceiver, TaskPluginCollector taskPluginCollector, Connection connection) {
			this.taskPluginCollector = taskPluginCollector;

			// 用于写入数据的时候的类型根据目的表字段类型转换
			this.resultSetMetaData = DBUtil.getColumnMetaData(connection,
					this.table, StringUtils.join(this.columns, ","));
			// 写数据库的SQL语句
			calcWriteRecordSql();
			List<Record> writeBuffer = new ArrayList<Record>(this.batchSize);
			int bufferBytes = 0;
			try {
				Record record;
				while ((record = recordReceiver.getFromReader()) != null) {
					if (record.getColumnNumber() != this.columnNumber) {
						// 源头读取字段列数与目的表字段写入列数不相等，直接报错
						throw DataXException
								.asDataXException(
										DBUtilErrorCode.CONF_ERROR,
										String.format(
												"列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 请检查您的配置并作出修改.",
												record.getColumnNumber(),
												this.columnNumber));
					}

					writeBuffer.add(record);
					bufferBytes += record.getMemorySize();

					if (writeBuffer.size() >= batchSize || bufferBytes >= batchByteSize) {
						insert(connection, writeBuffer);
						writeBuffer.clear();
						bufferBytes = 0;
					}
				}
				if (!writeBuffer.isEmpty()) {
					insert(connection, writeBuffer);
					writeBuffer.clear();
					bufferBytes = 0;
				}
			} catch (Exception e) {
				throw DataXException.asDataXException(
						DBUtilErrorCode.WRITE_DATA_ERROR, e);
			} finally {
				writeBuffer.clear();
				bufferBytes = 0;
				DBUtil.closeDBResources(null, null, connection);
			}
		}

		// TODO 改用连接池，确保每次获取的连接都是可用的（注意：连接可能需要每次都初始化其 session）
		public void startWrite(RecordReceiver recordReceiver,
							   Configuration writerSliceConfig,
							   TaskPluginCollector taskPluginCollector) {
			Connection connection = DBUtil.getConnection(this.dataBaseType,
					this.jdbcUrl, username, password);
			DBUtil.dealWithSessionConfig(connection, writerSliceConfig,
					this.dataBaseType, BASIC_MESSAGE);
			startWriteWithConnection(recordReceiver, taskPluginCollector, connection);
		}


		public void post(Configuration writerSliceConfig) {
			int tableNumber = writerSliceConfig.getInt(
					Constant.TABLE_NUMBER_MARK);

			boolean hasPostSql = (this.postSqls != null && this.postSqls.size() > 0);
			if (tableNumber == 1 || !hasPostSql) {
				return;
			}

			Connection connection = DBUtil.getConnection(this.dataBaseType,
					this.jdbcUrl, username, password);

			LOG.info("Begin to execute postSqls:[{}]. context info:{}.",
					StringUtils.join(this.postSqls, ";"), BASIC_MESSAGE);
			WriterUtil.executeSqls(connection, this.postSqls, BASIC_MESSAGE, dataBaseType);
			DBUtil.closeDBResources(null, null, connection);
		}

		public void destroy(Configuration writerSliceConfig) {
		}

		protected void insert(Connection connection, List<Record> buffer)
				throws SQLException {
			PreparedStatement preparedStatement = null;
			try {
				if (buffer.size() != batchSize){
					String sqlTemplate = WriterUtil.getWriteTemplate(columns, primaryKeys, writeMode,buffer.size());
					preparedStatement = connection.prepareStatement(String.format(sqlTemplate, this.table));
				} else {
					preparedStatement = connection.prepareStatement(this.writeRecordSql);
				}
				int batchIndex = 0;
				for (Record record : buffer) {
					preparedStatement = fillPreparedStatement(
							preparedStatement, connection, record, batchIndex);
					++batchIndex;
					arrayIndex = 0;
				}
				preparedStatement.execute();
			} catch (SQLException e) {
				LOG.warn("执行query失败，因为：" + e.getMessage());

			} catch (Exception e) {
				throw DataXException.asDataXException(
						DBUtilErrorCode.WRITE_DATA_ERROR, e);
			} finally {
				DBUtil.closeDBResources(preparedStatement, null);
			}
		}


		// 直接使用了两个类变量：columnNumber,resultSetMetaData
		protected PreparedStatement fillPreparedStatement(PreparedStatement preparedStatement, Connection connection, Record record, int batchIndex)
				throws SQLException {
			for (int i = 0; i < this.columnNumber; i++) {
				int columnSqltype = this.resultSetMetaData.getMiddle().get(i);
				preparedStatement = fillPreparedStatementColumnType(preparedStatement, connection, i, batchIndex, columnSqltype, record.getColumn(i));
			}

			return preparedStatement;
		}

		protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, Connection connection, int columnIndex,int batchIndex, int columnSqltype, Column column) throws SQLException {
			java.util.Date utilDate;
			switch (columnSqltype) {
				case Types.CHAR:
					String charValue = column.asString();
					if (emptyAsNull && "".equals(charValue)){
						preparedStatement.setNull(batchIndex * columnNumber + columnIndex + 1, Types.CHAR);
						break;
					} else {
						preparedStatement.setString(batchIndex * columnNumber + columnIndex + 1, column
								.asString());
					}
					break;
				case Types.NCHAR:
				case Types.CLOB:
				case Types.NCLOB:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
				case Types.NVARCHAR:
				case Types.LONGNVARCHAR:
					String varcharValue = column.asString();
					if (null == varcharValue || (emptyAsNull && "".equals(varcharValue))){
						preparedStatement.setNull(batchIndex * columnNumber + columnIndex + 1, Types.VARCHAR);
					} else {
						preparedStatement.setString(batchIndex * columnNumber + columnIndex + 1, column
								.asString());
					}
					break;

				case Types.SMALLINT:
					String smallIntValue = column.asString();
					if (null == smallIntValue || (emptyAsNull && "".equals(smallIntValue))){
						preparedStatement.setNull(batchIndex * columnNumber + columnIndex + 1, Types.SMALLINT);
					} else {
						preparedStatement.setInt(batchIndex * columnNumber + columnIndex + 1, Integer.parseInt(smallIntValue));
					}
					break;
				case Types.INTEGER:
					String intValue = column.asString();
					if (null == intValue || (emptyAsNull && "".equals(intValue))){
						preparedStatement.setNull(batchIndex * columnNumber + columnIndex + 1, Types.INTEGER);
					} else {
						preparedStatement.setInt(batchIndex * columnNumber + columnIndex + 1, Integer.parseInt(intValue));
					}
					break;
				case Types.BIGINT:
					String longValue = column.asString();
					if ( null == longValue ||(emptyAsNull && "".equals(longValue))){
						preparedStatement.setNull(batchIndex * columnNumber + columnIndex + 1, Types.BIGINT);
					} else {
						preparedStatement.setLong(batchIndex * columnNumber + columnIndex + 1, Long.parseLong(longValue));
					}
					break;
				case Types.NUMERIC:
				case Types.DECIMAL:
					String decimalValue = column.asString();
					if (null == decimalValue || (emptyAsNull && "".equals(decimalValue))) {
						preparedStatement.setNull(batchIndex * columnNumber + columnIndex + 1, Types.NUMERIC);
					} else {
						preparedStatement.setBigDecimal(batchIndex * columnNumber + columnIndex + 1, new BigDecimal(decimalValue));
					}
					break;
				case Types.FLOAT:
				case Types.REAL:
					String floatValue = column.asString();
					if (null == floatValue || (emptyAsNull && "".equals(floatValue))) {
						preparedStatement.setNull(batchIndex * columnNumber + columnIndex + 1, Types.REAL);
					} else {
						preparedStatement.setFloat(batchIndex * columnNumber + columnIndex + 1, Float.parseFloat(floatValue));
					}
					break;
				case Types.DOUBLE:
					String doubleValue = column.asString();
					if ( null == doubleValue|| (emptyAsNull && "".equals(doubleValue))) {
						preparedStatement.setNull(batchIndex * columnNumber + columnIndex + 1, Types.DOUBLE);
					} else {
						preparedStatement.setDouble(batchIndex * columnNumber + columnIndex + 1, Double.parseDouble(doubleValue));
					}
					break;

				//tinyint is a little special in some database like mysql {boolean->tinyint(1)}
				case Types.TINYINT:
					Long tinyValue = column.asLong();
					if (null == tinyValue) {
						preparedStatement.setNull(batchIndex * columnNumber + columnIndex + 1, Types.TINYINT);
					} else {
						preparedStatement.setString(batchIndex * columnNumber + columnIndex + 1, tinyValue.toString());
					}
					break;
				case Types.DATE:
					if (this.resultSetMetaData.getRight().get(columnIndex)
							.equalsIgnoreCase("year")) {
						if (column.asBigInteger() == null) {
							preparedStatement.setNull(batchIndex * columnNumber + columnIndex + 1, Types.DATE);
						} else {
							preparedStatement.setInt(batchIndex * columnNumber + columnIndex + 1, column.asBigInteger().intValue());
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
						if (sqlDate == null){
							preparedStatement.setNull(batchIndex * columnNumber + columnIndex + 1, Types.DATE);
						} else {
							preparedStatement.setDate(batchIndex * columnNumber + columnIndex + 1, sqlDate);
						}
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
					if (null == sqlTime){
						preparedStatement.setNull(batchIndex * columnNumber + columnIndex + 1, Types.TIME);
					} else {
						preparedStatement.setTime(batchIndex * columnNumber + columnIndex + 1, sqlTime);
					}
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
					if (null == sqlTimestamp){
						preparedStatement.setNull(batchIndex * columnNumber + columnIndex + 1, Types.TIMESTAMP);
					} else {
						preparedStatement.setTimestamp(batchIndex * columnNumber + columnIndex + 1, sqlTimestamp);
					}
					break;

				case Types.BINARY:
				case Types.VARBINARY:
				case Types.BLOB:
				case Types.LONGVARBINARY:
					String byteValue = column.asString();
					if (null == byteValue || (emptyAsNull && "".equals(byteValue))) {
						preparedStatement.setNull(batchIndex * columnNumber + columnIndex + 1, Types.BINARY);
					} else {
						preparedStatement.setBytes(batchIndex * columnNumber + columnIndex + 1, column
								.asBytes());
					}
					break;
				case Types.BOOLEAN:
				case Types.BIT:
					String boolValue = column.asString();
					if (null == boolValue || (emptyAsNull && "".equals(boolValue))) {
						preparedStatement.setNull(batchIndex * columnNumber + columnIndex + 1, Types.BIT);
					} else {
						preparedStatement.setBoolean(batchIndex * columnNumber + columnIndex + 1, Boolean.parseBoolean(boolValue));
					}
					break;
				case Types.ARRAY:
					if (null == arrayTypes || arrayTypes.isEmpty()){
						throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
								String.format("您写入的类型包含数组，但缺少元素类型，请在配置中指定元素类型"));
					}
					String arrayString = column.asString();
					String[] arrayValues;
					if (null == arrayString || (emptyAsNull && "".equals(arrayString))) {
						preparedStatement.setNull(batchIndex * columnNumber + columnIndex + 1, Types.ARRAY);
						break;
					} else {
						arrayValues = arrayString.split(this.arrayDelimiter);
					}
					if (!arrayTypes.get(arrayIndex).isEmpty()){
						if (arrayTypes.get(arrayIndex).equalsIgnoreCase("text") || arrayTypes.get(arrayIndex).equalsIgnoreCase("varchar")){
							Object[] array = new Object[arrayValues.length];
							for (int i = 0; i < array.length; ++i){
								array[i] = arrayValues[i];
							}
							preparedStatement.setArray(batchIndex * columnNumber + columnIndex + 1, connection.createArrayOf("text", array));
						} else if (arrayTypes.get(arrayIndex).equalsIgnoreCase("smallint")){
							Object[] array = new Object[arrayValues.length];
							for (int i = 0; i < array.length; ++i){
								array[i] = Integer.parseInt(arrayValues[i]);
							}
							preparedStatement.setArray(batchIndex * columnNumber + columnIndex + 1, connection.createArrayOf("int2", array));
						} else if (arrayTypes.get(arrayIndex).equalsIgnoreCase("integer")){
							Object[] array = new Object[arrayValues.length];
							for (int i = 0; i < array.length; ++i){
								array[i] = Integer.parseInt(arrayValues[i]);
							}
							preparedStatement.setArray(batchIndex * columnNumber + columnIndex + 1, connection.createArrayOf("int4", array));
						} else if (arrayTypes.get(arrayIndex).equalsIgnoreCase("bigint")){
							Object[] array = new Object[arrayValues.length];
							for (int i = 0; i < array.length; ++i){
								array[i] = Long.parseLong(arrayValues[i]);
							}
							preparedStatement.setArray(batchIndex * columnNumber + columnIndex + 1, connection.createArrayOf("int8", array));
						} else if (arrayTypes.get(arrayIndex).equalsIgnoreCase("real")){
							Object[] array = new Object[arrayValues.length];
							for (int i = 0; i < array.length; ++i){
								array[i] = Float.parseFloat(arrayValues[i]);
							}
							preparedStatement.setArray(batchIndex * columnNumber + columnIndex + 1, connection.createArrayOf("float4", array));
						} else if (arrayTypes.get(arrayIndex).equalsIgnoreCase("double")){
							Object[] array = new Object[arrayValues.length];
							for (int i = 0; i < array.length; ++i){
								array[i] = Double.parseDouble(arrayValues[i]);
							}
							preparedStatement.setArray(batchIndex * columnNumber + columnIndex + 1, connection.createArrayOf("float8", array));
						} else if (arrayTypes.get(arrayIndex).equalsIgnoreCase("numeric") || arrayTypes.get(arrayIndex).equalsIgnoreCase("decimal")){
							Object[] array = new Object[arrayValues.length];
							for (int i = 0; i < array.length; ++i){
								array[i] = new BigDecimal(arrayValues[i]);
							}
							preparedStatement.setArray(batchIndex * columnNumber + columnIndex + 1, connection.createArrayOf("numeric", array));
						} else if (arrayTypes.get(arrayIndex).equalsIgnoreCase("bool") || arrayTypes.get(arrayIndex).equalsIgnoreCase("boolean")){
							Object[] array = new Object[arrayValues.length];
							for (int i = 0; i < array.length; ++i){
								array[i] = Boolean.parseBoolean(arrayValues[i]);
							}
							preparedStatement.setArray(batchIndex * columnNumber + columnIndex + 1, connection.createArrayOf("bool", array));
						} else {
							throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
									String.format("您配置的数组类型 [%s]，HologresJdbcWriter暂不支持.", arrayTypes.get(arrayIndex)));
						}
					} else{
						throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
								String.format("元素类型不能为空，请在配置中正确指定元素类型"));
					}
					++arrayIndex;
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

		private void calcWriteRecordSql() {
			INSERT_OR_REPLACE_TEMPLATE = WriterUtil.getWriteTemplate(columns, primaryKeys, writeMode,batchSize);
			writeRecordSql = String.format(INSERT_OR_REPLACE_TEMPLATE, this.table);
		}
	}
}
