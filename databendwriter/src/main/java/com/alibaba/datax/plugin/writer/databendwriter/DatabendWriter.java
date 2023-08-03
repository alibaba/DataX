package com.alibaba.datax.plugin.writer.databendwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.writer.databendwriter.util.DatabendWriterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;
import java.util.regex.Pattern;

public class DatabendWriter extends Writer {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.Databend;

    public static class Job
            extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration originalConfig;
        private CommonRdbmsWriter.Job commonRdbmsWriterMaster;

        @Override
        public void init() throws DataXException {
            this.originalConfig = super.getPluginJobConf();
            this.commonRdbmsWriterMaster = new CommonRdbmsWriter.Job(DATABASE_TYPE);
            this.commonRdbmsWriterMaster.init(this.originalConfig);
            // placeholder currently not supported by databend driver, needs special treatment
            DatabendWriterUtil.dealWriteMode(this.originalConfig);
        }

        @Override
        public void preCheck() {
            this.init();
            this.commonRdbmsWriterMaster.writerPreCheck(this.originalConfig, DATABASE_TYPE);
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
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;

        private CommonRdbmsWriter.Task commonRdbmsWriterSlave;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();

            this.commonRdbmsWriterSlave = new CommonRdbmsWriter.Task(DataBaseType.Databend) {
                @Override
                protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex, int columnSqltype, String typeName, Column column) throws SQLException {
                    try {
                        if (column.getRawData() == null) {
                            preparedStatement.setNull(columnIndex + 1, columnSqltype);
                            return preparedStatement;
                        }

                        java.util.Date utilDate;
                        switch (columnSqltype) {

                            case Types.TINYINT:
                            case Types.SMALLINT:
                            case Types.INTEGER:
                                preparedStatement.setInt(columnIndex + 1, column.asBigInteger().intValue());
                                break;
                            case Types.BIGINT:
                                preparedStatement.setLong(columnIndex + 1, column.asLong());
                                break;
                            case Types.DECIMAL:
                                preparedStatement.setBigDecimal(columnIndex + 1, column.asBigDecimal());
                                break;
                            case Types.FLOAT:
                            case Types.REAL:
                                preparedStatement.setFloat(columnIndex + 1, column.asDouble().floatValue());
                                break;
                            case Types.DOUBLE:
                                preparedStatement.setDouble(columnIndex + 1, column.asDouble());
                                break;
                            case Types.DATE:
                                java.sql.Date sqlDate = null;
                                try {
                                    utilDate = column.asDate();
                                } catch (DataXException e) {
                                    throw new SQLException(String.format(
                                            "Date type conversion error: [%s]", column));
                                }

                                if (null != utilDate) {
                                    sqlDate = new java.sql.Date(utilDate.getTime());
                                }
                                preparedStatement.setDate(columnIndex + 1, sqlDate);
                                break;

                            case Types.TIME:
                                java.sql.Time sqlTime = null;
                                try {
                                    utilDate = column.asDate();
                                } catch (DataXException e) {
                                    throw new SQLException(String.format(
                                            "Date type conversion error: [%s]", column));
                                }

                                if (null != utilDate) {
                                    sqlTime = new java.sql.Time(utilDate.getTime());
                                }
                                preparedStatement.setTime(columnIndex + 1, sqlTime);
                                break;

                            case Types.TIMESTAMP:
                                Timestamp sqlTimestamp = null;
                                if (column instanceof StringColumn && column.asString() != null) {
                                    String timeStampStr = column.asString();
                                    // JAVA TIMESTAMP 类型入参必须是 "2017-07-12 14:39:00.123566" 格式
                                    String pattern = "^\\d+-\\d+-\\d+ \\d+:\\d+:\\d+.\\d+";
                                    boolean isMatch = Pattern.matches(pattern, timeStampStr);
                                    if (isMatch) {
                                        sqlTimestamp = Timestamp.valueOf(timeStampStr);
                                        preparedStatement.setTimestamp(columnIndex + 1, sqlTimestamp);
                                        break;
                                    }
                                }
                                try {
                                    utilDate = column.asDate();
                                } catch (DataXException e) {
                                    throw new SQLException(String.format(
                                            "Date type conversion error: [%s]", column));
                                }

                                if (null != utilDate) {
                                    sqlTimestamp = new Timestamp(
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

                                // warn: bit(1) -> Types.BIT 可使用setBoolean
                                // warn: bit(>1) -> Types.VARBINARY 可使用setBytes
                            case Types.BIT:
                                if (this.dataBaseType == DataBaseType.MySql) {
                                    Boolean asBoolean = column.asBoolean();
                                    if (asBoolean != null) {
                                        preparedStatement.setBoolean(columnIndex + 1, asBoolean);
                                    } else {
                                        preparedStatement.setNull(columnIndex + 1, Types.BIT);
                                    }
                                } else {
                                    preparedStatement.setString(columnIndex + 1, column.asString());
                                }
                                break;

                            default:
                                // cast variant / array into string is fine.
                                preparedStatement.setString(columnIndex + 1, column.asString());
                                break;
                        }
                        return preparedStatement;
                    } catch (DataXException e) {
                        // fix类型转换或者溢出失败时，将具体哪一列打印出来
                        if (e.getErrorCode() == CommonErrorCode.CONVERT_NOT_SUPPORT ||
                                e.getErrorCode() == CommonErrorCode.CONVERT_OVER_FLOW) {
                            throw DataXException
                                    .asDataXException(
                                            e.getErrorCode(),
                                            String.format(
                                                    "type conversion error. columnName: [%s], columnType:[%d], columnJavaType: [%s]. please change the data type in given column field or do not sync on the column.",
                                                    this.resultSetMetaData.getLeft()
                                                            .get(columnIndex),
                                                    this.resultSetMetaData.getMiddle()
                                                            .get(columnIndex),
                                                    this.resultSetMetaData.getRight()
                                                            .get(columnIndex)));
                        } else {
                            throw e;
                        }
                    }
                }

            };
            this.commonRdbmsWriterSlave.init(this.writerSliceConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterSlave.destroy(this.writerSliceConfig);
        }

        @Override
        public void prepare() {
            this.commonRdbmsWriterSlave.prepare(this.writerSliceConfig);
        }

        @Override
        public void post() {
            this.commonRdbmsWriterSlave.post(this.writerSliceConfig);
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            this.commonRdbmsWriterSlave.startWrite(lineReceiver, this.writerSliceConfig, this.getTaskPluginCollector());
        }

    }
}
