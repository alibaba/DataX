package com.alibaba.datax.plugin.writer.postgresqlwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Key;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

public class PostgresqlWriter extends Writer {
	private static final DataBaseType DATABASE_TYPE = DataBaseType.PostgreSQL;

	public static class Job extends Writer.Job {
		private Configuration originalConfig = null;
		private CommonRdbmsWriter.Job commonRdbmsWriterMaster;

		@Override
		public void init() {
			this.originalConfig = super.getPluginJobConf();
			this.commonRdbmsWriterMaster = new CommonRdbmsWriter.Job(DATABASE_TYPE);
			this.commonRdbmsWriterMaster.init(this.originalConfig);
		}

		@Override
		public void prepare() {
			this.commonRdbmsWriterMaster.prepare(this.originalConfig);
		}

		@Override
		public List<Configuration> split(int mandatoryNumber) {
			return this.commonRdbmsWriterMaster.split(this.originalConfig, mandatoryNumber);
		}

		@Override
		public void post() {
			this.commonRdbmsWriterMaster.post(this.originalConfig);
		}

		@Override
		public void destroy() {
			this.commonRdbmsWriterMaster.destroy(this.originalConfig);
		}

	}

	public static class Task extends Writer.Task {
		private Configuration writerSliceConfig;
		private CommonRdbmsWriter.Task commonRdbmsWriterSlave;

		@Override
		public void init() {
			this.writerSliceConfig = super.getPluginJobConf();
			this.commonRdbmsWriterSlave = new CommonRdbmsWriter.Task(DATABASE_TYPE){
				@Override
				public String calcValueHolder(String columnType){
					if("serial".equalsIgnoreCase(columnType)){
						return "?::int";
					}else if("bigserial".equalsIgnoreCase(columnType)){
						return "?::int8";
					}else if("bit".equalsIgnoreCase(columnType)){
						return "?::bit varying";
					}
					return "?::" + columnType;
				}

				@Override
				public  PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex,
																		  int columnSqltype, String typeName, Column column) throws SQLException {
					java.util.Date utilDate;

					boolean forceUseUpdate =  this.writeMode.trim().toLowerCase().startsWith("update");
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
							if (forceUseUpdate) {
								preparedStatement.setString(columnIndex + 1 + columnNumber, column
										.asString());
							}

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
								if (forceUseUpdate) {
									preparedStatement.setString(columnIndex + 1 + columnNumber, null);
								}
							} else {
								preparedStatement.setString(columnIndex + 1, strValue);
								if (forceUseUpdate) {
									preparedStatement.setString(columnIndex + 1 + columnNumber, strValue);
								}
							}
							break;

						//tinyint is a little special in some database like mysql {boolean->tinyint(1)}
						case Types.TINYINT:
							Long longValue = column.asLong();
							if (null == longValue) {
								preparedStatement.setString(columnIndex + 1, null);
								if (forceUseUpdate) {
									preparedStatement.setString(columnIndex + 1 + columnNumber, null);
								}

							} else {
								preparedStatement.setString(columnIndex + 1, longValue.toString());
								if (forceUseUpdate) {
									preparedStatement.setString(columnIndex + 1 + columnNumber, longValue.toString());
								}
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
									if (forceUseUpdate) {
										preparedStatement.setString(columnIndex + 1 + columnNumber, null);
									}
								} else {
									preparedStatement.setInt(columnIndex + 1, column.asBigInteger().intValue());
									if (forceUseUpdate) {
										preparedStatement.setInt(columnIndex + 1 + columnNumber, column.asBigInteger().intValue());
									}
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
								if (forceUseUpdate) {
									preparedStatement.setDate(columnIndex + 1 + columnNumber, sqlDate);
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
							preparedStatement.setTime(columnIndex + 1, sqlTime);
							if (forceUseUpdate) {
								preparedStatement.setTime(columnIndex + 1 + columnNumber,sqlTime);
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
							preparedStatement.setTimestamp(columnIndex + 1, sqlTimestamp);
							if (forceUseUpdate) {
								preparedStatement.setTimestamp(columnIndex + 1 + columnNumber, sqlTimestamp);
							}
							break;

						case Types.BINARY:
						case Types.VARBINARY:
						case Types.BLOB:
						case Types.LONGVARBINARY:
							preparedStatement.setBytes(columnIndex + 1, column
									.asBytes());
							if (forceUseUpdate) {
								preparedStatement.setBytes(columnIndex + 1 + columnNumber, column
										.asBytes());
							}
							break;
						case Types.BOOLEAN:
							preparedStatement.setBoolean(columnIndex + 1, column.asBoolean());
							if (forceUseUpdate) {
								preparedStatement.setBoolean(columnIndex + 1 + columnNumber, column.asBoolean());
							}
							break;

						// warn: bit(1) -> Types.BIT 可使用setBoolean
						// warn: bit(>1) -> Types.VARBINARY 可使用setBytes
						case Types.BIT:
							if (this.dataBaseType == DataBaseType.MySql) {
								preparedStatement.setBoolean(columnIndex + 1, column.asBoolean());
								if (forceUseUpdate) {
									preparedStatement.setBoolean(columnIndex + 1 + columnNumber, column.asBoolean());
								}
							} else {
								preparedStatement.setString(columnIndex + 1, column.asString());
								if (forceUseUpdate) {
									preparedStatement.setString(columnIndex + 1 + columnNumber, column.asString());
								}
							}
							break;
						case Types.OTHER:
							preparedStatement.setString(columnIndex + 1, column.asString());
							if (forceUseUpdate) {
								preparedStatement.setString(columnIndex + 1 + columnNumber, column.asString());
							}
							break;
						case Types.ARRAY:
							Object rawData = column.getRawData();
							if (rawData == null)  {
								preparedStatement.setArray(columnIndex + 1,null);
								if (forceUseUpdate) {
									preparedStatement.setArray(columnIndex + 1 + columnNumber, null);
								}
								break;
							}
							String wType = this.resultSetMetaData.getRight()
									.get(columnIndex);
							Array data = null;
							switch (wType) {
								case "_varchar":
									data = preparedStatement.getConnection().createArrayOf("VARCHAR", (String[])rawData);
									break;
								case "_int8":
									data = preparedStatement.getConnection().createArrayOf("LONG", (Long[])rawData);
									break;
								case "_int4":
									data = preparedStatement.getConnection().createArrayOf("INTEGER", (Integer[])rawData);
									break;
								default:
									data = preparedStatement.getConnection().createArrayOf("VARCHAR", (String[])rawData);
							}
							preparedStatement.setArray(columnIndex + 1,data);
							if (forceUseUpdate) {
								preparedStatement.setArray(columnIndex + 1 + columnNumber, data);
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

			};
			this.commonRdbmsWriterSlave.init(this.writerSliceConfig);
		}

		@Override
		public void prepare() {
			this.commonRdbmsWriterSlave.prepare(this.writerSliceConfig);
		}

		public void startWrite(RecordReceiver recordReceiver) {
			this.commonRdbmsWriterSlave.startWrite(recordReceiver, this.writerSliceConfig, super.getTaskPluginCollector());
		}

		@Override
		public void post() {
			this.commonRdbmsWriterSlave.post(this.writerSliceConfig);
		}

		@Override
		public void destroy() {
			this.commonRdbmsWriterSlave.destroy(this.writerSliceConfig);
		}

	}

}
