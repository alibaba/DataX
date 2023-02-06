package com.alibaba.datax.plugin.writer.clickhousewriter;

import java.sql.*;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.fastjson.JSONObject;

/**
 * @author : donghao
 * @version : 1.0
 * @className : ClickHouseWriterTask
 * @description: 实现与ClickHouse匹配的WriterTask
 * @date : 2022-08-11 16:22
 */
public class ClickHouseWriterTask extends CommonRdbmsWriter.Task{
    protected static final Logger LOG = LoggerFactory.getLogger(ClickHouseWriterTask.class);

    public ClickHouseWriterTask(DataBaseType dataBaseType) {
        super(dataBaseType);
    }

    @Override
    protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex,
                                                                int columnSqltype, String typeName, Column column) throws SQLException {
        if (column.getRawData() == null) {
            preparedStatement.setNull(columnIndex + 1, columnSqltype);
            return preparedStatement;
        }
        java.util.Date utilDate;
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
                break;
            case Types.TINYINT:
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
                } else {
                    preparedStatement.setString(columnIndex + 1, strValue);
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
                    } else {
                        preparedStatement.setInt(columnIndex + 1, column.asBigInteger().intValue());
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
                break;

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.BLOB:
            case Types.LONGVARBINARY:
                preparedStatement.setBytes(columnIndex + 1, column
                        .asBytes());
                break;

            case Types.BOOLEAN:
                preparedStatement.setBoolean(columnIndex + 1, column.asBoolean());
                break;

            // warn: bit(1) -> Types.BIT 可使用setBoolean
            // warn: bit(>1) -> Types.VARBINARY 可使用setBytes
            case Types.BIT:
                if (this.dataBaseType == DataBaseType.MySql) {
                    preparedStatement.setBoolean(columnIndex + 1, column.asBoolean());
                } else {
                    preparedStatement.setString(columnIndex + 1, column.asString());
                }
                break;

            case Types.ARRAY:
                Connection conn = preparedStatement.getConnection();
                List<Object> values = JSON.parseArray(column.asString(), Object.class);
                for (int i = 0; i < values.size(); i++) {
                    values.set(i, this.toJavaArray(values.get(i)));
                }
                Array array = conn.createArrayOf("String", values.toArray());
                preparedStatement.setArray(columnIndex + 1, array);
                break;
            case Types.OTHER:
                // 处理ck其他类型
                // 1.Map类型
                if (this.dataBaseType == DataBaseType.ClickHouse) {
                    String columnStr = column.asString();
                    // 处理空map
                    if (column.getByteSize() == 2 && columnStr.equals("{}")) {
                        preparedStatement.setObject(columnIndex + 1, null);
                    }
                    // 处理正常map
                    else if (column.getByteSize() > 2 && columnStr.startsWith("{") && columnStr.endsWith("}")){
                        Map newMap = JSONObject.parseObject(columnStr, Map.class);
                        preparedStatement.setObject(columnIndex + 1, newMap);
                    }
                    // 其他情况按照字符串处理
                    else {
                        preparedStatement.setObject(columnIndex + 1, columnStr);
                    }
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

    private Object toJavaArray(Object val) {
        if (null == val) {
            return null;
        } else if (val instanceof JSONArray) {
            Object[] valArray = ((JSONArray) val).toArray();
            for (int i = 0; i < valArray.length; i++) {
                valArray[i] = this.toJavaArray(valArray[i]);
            }
            return valArray;
        } else {
            return val;
        }
    }

}
