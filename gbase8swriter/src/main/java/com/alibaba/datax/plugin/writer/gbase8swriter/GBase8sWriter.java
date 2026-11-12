package com.alibaba.datax.plugin.writer.gbase8swriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Key;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

public class GBase8sWriter extends Writer {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.GBase8s;

    public static class Job extends Writer.Job {
        private Configuration originalConfig = null;
        private CommonRdbmsWriter.Job commonRdbmsWriterMaster;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            String writeMode = this.originalConfig.getString(Key.WRITE_MODE);
            if (null != writeMode) {
                throw DataXException
                        .asDataXException(
                                DBUtilErrorCode.CONF_ERROR,
                                String.format(
                                        "写入模式(writeMode)配置有误. 因为不支持配置参数项 writeMode: %s, 仅使用insert sql 插入数据. 请检查您的配置并作出修改.",
                                        writeMode));
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
                protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex,
                                                                            int columnSqltype, String typeName, Column column) throws SQLException {
                    java.util.Date utilDate;
                    switch (columnSqltype) {
                        case Types.CHAR:
                        case Types.NCHAR:
                        case Types.CLOB:
                        case Types.NCLOB:
                        case Types.VARCHAR:
                        case 63:
                        case 64:
                        case Types.LONGVARCHAR:
                        case Types.NVARCHAR:
                        case Types.LONGNVARCHAR:
                            preparedStatement.setString(columnIndex + 1, column
                                    .asString());
                            break;

                        case Types.SMALLINT:
                        case Types.INTEGER:
                        case Types.BIGINT:
                        case 66:
                        case 68:
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

                        //tinyint is a little special in some database like mysql {boolean->tinyint(1)}
                        case Types.TINYINT:
                            Long longValue = column.asLong();
                            if (null == longValue) {
                                preparedStatement.setString(columnIndex + 1, null);
                            } else {
                                preparedStatement.setString(columnIndex + 1, longValue.toString());
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