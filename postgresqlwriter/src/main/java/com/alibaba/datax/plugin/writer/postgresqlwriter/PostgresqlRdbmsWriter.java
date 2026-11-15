package com.alibaba.datax.plugin.writer.postgresqlwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.PostgisWrapper;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.fastjson2.JSON;
import org.apache.commons.lang3.StringUtils;
import org.postgis.PGgeometry;
import org.postgresql.util.PGobject;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PostgisRdbmsWriter extends CommonRdbmsWriter {

    public static class Job extends CommonRdbmsWriter.Job {
        public Job(DataBaseType dataBaseType) {
            super(dataBaseType);
        }
    }

    public static class Task extends CommonRdbmsWriter.Task {

        private List<String> uniqueKeyColumnNames;

        public Task(DataBaseType dataBaseType) {
            super(dataBaseType);
        }

        @Override
        public void init(Configuration readerSliceConfig) {
            super.init(readerSliceConfig);
            this.uniqueKeyColumnNames = readerSliceConfig.getList(
                    Key.UNIQUE_KEY, String.class);
        }

        @Override
        protected PreparedStatement fillPreparedStatementColumnType(
                PreparedStatement preparedStatement, int columnIndex,
                int columnSqltype, String typeName, Column column) throws SQLException {
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
                    case Types.ARRAY:
                        Object rawDataArray = column.getRawData();
                        if (Objects.isNull(rawDataArray)) {
                            preparedStatement.setNull(columnIndex + 1, Types.ARRAY);
                        } else {
                            if (rawDataArray instanceof byte[]) {
                                PostgisWrapper postgisWrapper = JSON.parseObject((byte[]) rawDataArray, PostgisWrapper.class);
                                if (Objects.nonNull(postgisWrapper)) {
                                    Object pgRawData = postgisWrapper.getRawData();
                                    String arrayStr = (String) pgRawData;
                                    if (arrayStr.startsWith("{") && arrayStr.endsWith("}")) {
                                        arrayStr = arrayStr.substring(1, arrayStr.length() - 1);
                                    }
                                    String[] elements = arrayStr.split(",");
                                    Object[] pgArray = new Object[elements.length];
                                    for (int i = 0; i < elements.length; i++) {
                                        pgArray[i] = elements[i].trim();
                                    }
                                    preparedStatement.setArray(columnIndex + 1,
                                            preparedStatement.getConnection().createArrayOf(
                                                    postgisWrapper.getColumnTypeName(), pgArray));
                                } else {
                                    LOG.error("type array deserialized PostgisWrapper is null, please check your database data.");
                                    preparedStatement.setObject(columnIndex + 1, rawDataArray);
                                }
                            }
                        }
                        break;
                    case Types.OTHER:
                        if (null == column.getRawData()) {
                            preparedStatement.setNull(columnIndex + 1, Types.JAVA_OBJECT);
                        } else {
                            Object rawData = column.getRawData();
                            if (rawData instanceof byte[]) {
                                PostgisWrapper postgisWrapper = JSON.parseObject((byte[]) rawData, PostgisWrapper.class);
                                if (Objects.nonNull(postgisWrapper)) {
                                    String columnTypeName = postgisWrapper.getColumnTypeName();
                                    Object pgData = postgisWrapper.getRawData();
                                    Object objToWrite;
                                    if (PostGisColumnTypeName.GEOMETRY.equals(columnTypeName)) {
                                        objToWrite = new PGgeometry(
                                                (String) pgData);
                                    } else if (PostGisColumnTypeName.isPGObject(columnTypeName)) {
                                        objToWrite = postgisWrapper.getRawData();
                                        assert objToWrite instanceof String;
                                        objToWrite = JSON.parseObject((String) objToWrite, PGobject.class);
                                    } else {
                                        throw new IllegalStateException("Unsupported PostGIS type to write: " + columnTypeName);
                                    }
                                    preparedStatement.setObject(columnIndex + 1, objToWrite);
                                } else {
                                    LOG.error("type other deserialized PostgisWrapper is null, please check your database data.");
                                }
                            } else {
                                LOG.error("PostgisWriter write column {} with type {} is not BytesColumn, please check your database data.",
                                        this.resultSetMetaData.getLeft().get(columnIndex),
                                        this.resultSetMetaData.getMiddle().get(columnIndex));
                                preparedStatement.setObject(columnIndex + 1, rawData);
                            }
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

        @Override
        protected void calcWriteRecordSql() {
            boolean isWriteModeLegal = writeMode.trim().toLowerCase().startsWith("insert")
                    || writeMode.trim().toLowerCase().startsWith("replace")
                    || writeMode.trim().toLowerCase().startsWith("update");

            if (!isWriteModeLegal) {
                throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                        String.format("您所配置的 writeMode:%s 错误. 因为DataX 目前仅支持replace,update 或 insert 方式. 请检查您的配置并作出修改.", writeMode));
            }
            List<String> valueHolders = new ArrayList<String>(columnNumber);
            for (int i = 0; i < columns.size(); i++) {
                String type = resultSetMetaData.getRight().get(i);
                valueHolders.add(calcValueHolder(type));
            }
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("INSERT INTO ")
                    .append(this.table)
                    .append(" (")
                    .append(StringUtils.join(this.columns, ","))
                    .append(") VALUES (")
                    .append(StringUtils.join(valueHolders, ","))
                    .append(")");

            if (this.writeMode.equalsIgnoreCase("update")
                    && Objects.nonNull(this.uniqueKeyColumnNames) && !this.uniqueKeyColumnNames.isEmpty()) {
                sqlBuilder.append(" ON CONFLICT (")
                        .append(StringUtils.join(this.uniqueKeyColumnNames, ","))
                        .append(")")
                        .append(" DO UPDATE SET ");
                for (String column : this.columns) {
                    if (this.uniqueKeyColumnNames.contains(column)) {
                        continue; // Skip unique key columns in update
                    }
                    sqlBuilder.append(column)
                            .append(" = EXCLUDED.")
                            .append(column)
                            .append(", ");
                }
                sqlBuilder.replace(sqlBuilder.length() - 2, sqlBuilder.length(), ""); // Remove last comma
            }
            this.writeRecordSql = sqlBuilder.toString();
        }
    }
}
