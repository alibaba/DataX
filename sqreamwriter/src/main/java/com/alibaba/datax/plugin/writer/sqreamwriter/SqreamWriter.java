package com.alibaba.datax.plugin.writer.sqreamwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Key;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

public class SqreamWriter extends Writer {
	private static final DataBaseType DATABASE_TYPE = DataBaseType.SQream;
	public enum SqreamTypeId {
		Bool(Types.BOOLEAN),
		Tinyint(Types.TINYINT),
		Smallint(Types.SMALLINT),
		Int(Types.INTEGER),
		Bigint(Types.BIGINT),
		Real(Types.REAL),
		Float(Types.DOUBLE),
		
		Date(Types.DATE),
		DateTime(Types.TIMESTAMP),
		Varchar(Types.VARCHAR),
		NVarchar(Types.NVARCHAR);
		
		private int value;
		
		SqreamTypeId(int value) {
			this.value = value;
		}
		
		public int getValue(){
			return value;
		}
		
		
	}
	public static class Job extends Writer.Job {
		private Configuration originalConfig = null;
		private CommonRdbmsWriter.Job commonRdbmsWriterMaster;

		@Override
		public void init() {

			this.originalConfig = super.getPluginJobConf();

			// warn：not like mysql, PostgreSQL only support insert mode, don't use
			String writeMode = this.originalConfig.getString(Key.WRITE_MODE);
			if (null != writeMode) {
				throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
					String.format("Insert is the only supported mode, not ", writeMode));
			}

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
			this.commonRdbmsWriterSlave = new CommonRdbmsWriter.Task(DATABASE_TYPE) {
				
				@Override
				public String calcValueHolder(String columnType){
					return "?";
				}

				@Override
				protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex, int columnSqltype, Column column) throws SQLException 
				{
					//java.util.Date utilDate;
					String typeName = this.resultSetMetaData.getRight().get(columnIndex);
					// System.out.println("*******  type name:" + typeName);
					switch (typeName) {
						case "Bool": // "ftBool"
		                    preparedStatement.setBoolean(columnIndex + 1, column.asBoolean());
		                    break;
						case "Tinyint": // "ftUByte"
		                    preparedStatement.setByte(columnIndex + 1, column.asBigInteger().byteValueExact());
		                    break;
						case "Smallint": // "ftShort"
		                    preparedStatement.setShort(columnIndex + 1, column.asBigInteger().shortValueExact());
		                    break;
						case "Int": // "ftInt"
		                    preparedStatement.setInt(columnIndex + 1, column.asBigInteger().intValueExact());
		                    break;
					 	case "Bigint": // "ftLong"
					 		//System.out.println("*******  Setting Bigint");
		                    preparedStatement.setLong(columnIndex + 1, column.asLong());
		                    break;
					 	case "Real": // "ftFloat"
		                    preparedStatement.setFloat(columnIndex + 1, column.asBigDecimal().floatValue());
		                    break;
					 	case "Float": // "ftDouble"
		                    preparedStatement.setDouble(columnIndex + 1, column.asDouble());
		                    break;
					 	case "Date": // "ftDate"
		                    preparedStatement.setDate(columnIndex + 1, new Date((long)column.getRawData()));
		                    break;
					 	case "DateTime": // "ftDateTime"
		                    preparedStatement.setTimestamp(columnIndex + 1, new Timestamp((long)column.getRawData()));
		                    break;
					 	case "Varchar": // "ftVarchar"
		                    preparedStatement.setString(columnIndex + 1, column.asString());
		                    break;
					 	case "NVarchar": // "ftBlob"
		                    preparedStatement.setString(columnIndex + 1, column.asString());
		                    break;
					 	default: 
					 		throw new SQLException("Bad column type name, got: " + typeName);
					}
			        return preparedStatement;
				}
					
			};  // end overriding CommonRdbmsWriter.Task stuff
			
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
