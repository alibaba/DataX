package com.alibaba.datax.plugin.reader.rdbmswriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class SubCommonRdbmsWriter extends CommonRdbmsWriter {
    static {
        DBUtil.loadDriverClass("writer", "rdbms");
    }

    public static class Job extends CommonRdbmsWriter.Job {
        public Job(DataBaseType dataBaseType) {
            super(dataBaseType);
        }
    }

    public static class Task extends CommonRdbmsWriter.Task {
        public Task(DataBaseType dataBaseType) {
            super(dataBaseType);
        }

        @Override
        protected PreparedStatement fillPreparedStatementColumnType(
                PreparedStatement preparedStatement, int columnIndex,
                int columnSqltype, Column column) throws SQLException {
            java.util.Date utilDate;
            try {
                switch (columnSqltype) {
                case Types.CHAR:
                case Types.NCHAR:
                case Types.CLOB:
                case Types.NCLOB:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    if (null == column.getRawData()) {
                        preparedStatement.setObject(columnIndex + 1, null);
                    } else {
                        preparedStatement.setString(columnIndex + 1,
                                column.asString());
                    }
                    break;

                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIGINT:
                case Types.TINYINT:
                    String strLongValue = column.asString();
                    if (emptyAsNull && "".equals(strLongValue)) {
                        preparedStatement.setObject(columnIndex + 1, null);
                    } else if (null == column.getRawData()) {
                        preparedStatement.setObject(columnIndex + 1, null);
                    } else {
                        preparedStatement.setLong(columnIndex + 1,
                                column.asLong());
                    }
                    break;
                case Types.NUMERIC:
                case Types.DECIMAL:
                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE:
                    String strValue = column.asString();
                    if (emptyAsNull && "".equals(strValue)) {
                        preparedStatement.setObject(columnIndex + 1, null);
                    } else if (null == column.getRawData()) {
                        preparedStatement.setObject(columnIndex + 1, null);
                    } else {
                        preparedStatement.setDouble(columnIndex + 1,
                                column.asDouble());
                    }
                    break;

                case Types.DATE:
                    java.sql.Date sqlDate = null;
                    utilDate = column.asDate();
                    if (null != utilDate) {
                        sqlDate = new java.sql.Date(utilDate.getTime());
                        preparedStatement.setDate(columnIndex + 1, sqlDate);
                    } else {
                        preparedStatement.setNull(columnIndex + 1, Types.DATE);
                    }
                    break;

                case Types.TIME:
                    java.sql.Time sqlTime = null;
                    utilDate = column.asDate();
                    if (null != utilDate) {
                        sqlTime = new java.sql.Time(utilDate.getTime());
                        preparedStatement.setTime(columnIndex + 1, sqlTime);
                    } else {
                        preparedStatement.setNull(columnIndex + 1, Types.TIME);
                    }
                    break;

                case Types.TIMESTAMP:
                    java.sql.Timestamp sqlTimestamp = null;
                    utilDate = column.asDate();
                    if (null != utilDate) {
                        sqlTimestamp = new java.sql.Timestamp(
                                utilDate.getTime());
                        preparedStatement.setTimestamp(columnIndex + 1,
                                sqlTimestamp);
                    } else {
                        preparedStatement.setNull(columnIndex + 1,
                                Types.TIMESTAMP);
                    }
                    break;

                case Types.BINARY:
                case Types.VARBINARY:
                case Types.BLOB:
                case Types.LONGVARBINARY:
                    if (null == column.getRawData()) {
                        preparedStatement.setObject(columnIndex + 1, null);
                    } else {
                        preparedStatement.setBytes(columnIndex + 1,
                                column.asBytes());
                    }
                    break;

                case Types.BOOLEAN:
                    if (null == column.getRawData()) {
                        preparedStatement.setNull(columnIndex + 1,
                                Types.BOOLEAN);
                    } else {
                        preparedStatement.setBoolean(columnIndex + 1,
                                column.asBoolean());
                    }
                    break;

                // warn: bit(1) -> Types.BIT 可使用setBoolean
                // warn: bit(>1) -> Types.VARBINARY 可使用setBytes
                case Types.BIT:
                    if (null == column.getRawData()) {
                        preparedStatement.setObject(columnIndex + 1, null);
                    } else if (this.dataBaseType == DataBaseType.MySql) {
                        preparedStatement.setBoolean(columnIndex + 1,
                                column.asBoolean());
                    } else {
                        preparedStatement.setString(columnIndex + 1,
                                column.asString());
                    }
                    break;
                default:
                    preparedStatement.setObject(columnIndex + 1,
                            column.getRawData());
                    break;
                }
            } catch (DataXException e) {
                throw new SQLException(String.format(
                        "类型转换错误:[%s] 字段名:[%s], 字段类型:[%d], 字段Java类型:[%s].",
                        column,
                        this.resultSetMetaData.getLeft().get(columnIndex),
                        this.resultSetMetaData.getMiddle().get(columnIndex),
                        this.resultSetMetaData.getRight().get(columnIndex)));
            }
            return preparedStatement;
        }
    }
}
