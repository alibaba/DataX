package com.alibaba.datax.plugin.rdbms.reader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

public class ResultSetReadProxy {
	private static final Logger LOG = LoggerFactory
			.getLogger(ResultSetReadProxy.class);

	private static final boolean IS_DEBUG = LOG.isDebugEnabled();
	private static final byte[] EMPTY_CHAR_ARRAY = new byte[0];

	//TODO
	public static void transportOneRecord(RecordSender recordSender, ResultSet rs, 
			ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding, 
			TaskPluginCollector taskPluginCollector) {
		Record record = recordSender.createRecord();

		try {
			for (int i = 1; i <= columnNumber; i++) {
				switch (metaData.getColumnType(i)) {

				case Types.CHAR:
				case Types.NCHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
				case Types.NVARCHAR:
				case Types.LONGNVARCHAR:
					String rawData;
					if(StringUtils.isBlank(mandatoryEncoding)){
						rawData = rs.getString(i);
					}else{
						rawData = new String((rs.getBytes(i) == null ? EMPTY_CHAR_ARRAY : 
							rs.getBytes(i)), mandatoryEncoding);
					}
					record.addColumn(new StringColumn(rawData));
					break;

				case Types.CLOB:
				case Types.NCLOB:
					record.addColumn(new StringColumn(rs.getString(i)));
					break;

				case Types.SMALLINT:
				case Types.TINYINT:
				case Types.INTEGER:
				case Types.BIGINT:
					record.addColumn(new LongColumn(rs.getString(i)));
					break;

				case Types.NUMERIC:
				case Types.DECIMAL:
					record.addColumn(new DoubleColumn(rs.getString(i)));
					break;

				case Types.FLOAT:
				case Types.REAL:
				case Types.DOUBLE:
					record.addColumn(new DoubleColumn(rs.getString(i)));
					break;

				case Types.TIME:
					record.addColumn(new DateColumn(rs.getTime(i)));
					break;

				// for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
				case Types.DATE:
					if (metaData.getColumnTypeName(i).equalsIgnoreCase("year")) {
						record.addColumn(new LongColumn(rs.getInt(i)));
					} else {
						record.addColumn(new DateColumn(rs.getDate(i)));
					}
					break;

				case Types.TIMESTAMP:
					record.addColumn(new DateColumn(rs.getTimestamp(i)));
					break;

				case Types.BINARY:
				case Types.VARBINARY:
				case Types.BLOB:
				case Types.LONGVARBINARY:
					record.addColumn(new BytesColumn(rs.getBytes(i)));
					break;

				// warn: bit(1) -> Types.BIT 可使用BoolColumn
				// warn: bit(>1) -> Types.VARBINARY 可使用BytesColumn
				case Types.BOOLEAN:
				case Types.BIT:
					record.addColumn(new BoolColumn(rs.getBoolean(i)));
					break;

				case Types.NULL:
					String stringData = null;
					if(rs.getObject(i) != null) {
						stringData = rs.getObject(i).toString();
					}
					record.addColumn(new StringColumn(stringData));
					break;

				// TODO 添加BASIC_MESSAGE
				default:
					throw DataXException
							.asDataXException(
									DBUtilErrorCode.UNSUPPORTED_TYPE,
									String.format(
											"您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. 字段名:[%s], 字段名称:[%s], 字段Java类型:[%s]. 请尝试使用数据库函数将其转换datax支持的类型 或者不同步该字段 .",
											metaData.getColumnName(i),
											metaData.getColumnType(i),
											metaData.getColumnClassName(i)));
				}
			}
		} catch (Exception e) {
			if (IS_DEBUG) {
				LOG.debug("read data " + record.toString()
						+ " occur exception:", e);
			}

			//TODO 这里识别为脏数据靠谱吗？
			taskPluginCollector.collectDirtyRecord(record, e);
			if (e instanceof DataXException) {
				throw (DataXException) e;
			}
		}

		recordSender.sendToWriter(record);
	}
}
