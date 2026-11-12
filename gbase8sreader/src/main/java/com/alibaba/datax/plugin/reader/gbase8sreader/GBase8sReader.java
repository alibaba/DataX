package com.alibaba.datax.plugin.reader.gbase8sreader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.List;

public class GBase8sReader extends Reader {
    public static final int DEFAULT_FETCH_SIZE = 1000;

    private static final DataBaseType DATABASE_TYPE = DataBaseType.GBase8s;

    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private Configuration originalConfig = null;
        private CommonRdbmsReader.Job commonRdbmsReaderJob;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            dealFetchSize(this.originalConfig);
            this.commonRdbmsReaderJob = new CommonRdbmsReader.Job(DATABASE_TYPE);
            this.commonRdbmsReaderJob.init(this.originalConfig);
        }

        private void dealFetchSize(Configuration originalConfig) {
            int fetchSize = originalConfig.getInt(
                    com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE,
                    DEFAULT_FETCH_SIZE);
            if (fetchSize < 1) {
                throw DataXException
                        .asDataXException(DBUtilErrorCode.REQUIRED_VALUE,
                                String.format("您配置的 fetchSize 有误，fetchSize:[%d] 值不能小于 1.",
                                        fetchSize));
            }
            originalConfig.set(
                    com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE,
                    fetchSize);
        }

        @Override
        public void preCheck() {
            init();
            this.commonRdbmsReaderJob.preCheck(this.originalConfig, DATABASE_TYPE);

        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return this.commonRdbmsReaderJob.split(this.originalConfig, adviceNumber);
        }

        @Override
        public void post() {
            this.commonRdbmsReaderJob.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderJob.destroy(this.originalConfig);
        }

    }

    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;
        private CommonRdbmsReader.Task commonRdbmsReaderTask;

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsReaderTask = new CommonRdbmsReader.Task(DATABASE_TYPE, super.getTaskGroupId(), super.getTaskId()){

                private final Logger LOG = LoggerFactory.getLogger(CommonRdbmsReader.Task.class);
                private final boolean IS_DEBUG = LOG.isDebugEnabled();

                @Override
                protected Record buildRecord(RecordSender recordSender, ResultSet rs, ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding,
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
                                case 63:
                                case 64:
                                case Types.LONGNVARCHAR:
                                    String rawData;
                                    if (StringUtils.isBlank(mandatoryEncoding)) {
                                        rawData = rs.getString(i);
                                    } else {
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
                                case 66:
                                case 68:
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
                                    if (rs.getObject(i) != null) {
                                        stringData = rs.getObject(i).toString();
                                    }
                                    record.addColumn(new StringColumn(stringData));
                                    break;

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
                        taskPluginCollector.collectDirtyRecord(record, e);
                        if (e instanceof DataXException) {
                            throw (DataXException) e;
                        }
                    }
                    return record;
                }
            };
            this.commonRdbmsReaderTask.init(this.readerSliceConfig);

        }

        @Override
        public void startRead(RecordSender recordSender) {
            int fetchSize = this.readerSliceConfig
                    .getInt(com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE);

            this.commonRdbmsReaderTask.startRead(this.readerSliceConfig, recordSender,
                    super.getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void post() {
            this.commonRdbmsReaderTask.post(this.readerSliceConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderTask.destroy(this.readerSliceConfig);
        }

    }

}
