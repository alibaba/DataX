package com.alibaba.datax.plugin.reader.postgresqlreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.reader.rdbmsreader.SubCommonRdbmsReader;
import com.alibaba.fastjson2.JSON;
import org.apache.commons.lang3.StringUtils;
import org.postgis.PGgeometry;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Objects;

public class PostgisRdbmsReader extends CommonRdbmsReader {

    public static class Job extends CommonRdbmsReader.Job {

        public Job(DataBaseType dataBaseType) {
            super(dataBaseType);
        }
    }

    public static class Task extends CommonRdbmsReader.Task {

        private static final Logger LOG = LoggerFactory.getLogger(SubCommonRdbmsReader.Task.class);
        private static final boolean IS_DEBUG = LOG.isDebugEnabled();

        public Task(DataBaseType dataBaseType) {
            super(dataBaseType);
        }

        public Task(DataBaseType dataBaseType, int taskGropuId, int taskId) {
            super(dataBaseType, taskGropuId, taskId);
        }

        @Override
        protected Record transportOneRecord(RecordSender recordSender, ResultSet rs, ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding, TaskPluginCollector taskPluginCollector) {
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
                            if (StringUtils.isBlank(mandatoryEncoding)) {
                                rawData = rs.getString(i);
                            } else {
                                rawData = new String(
                                        (rs.getBytes(i) == null ? EMPTY_CHAR_ARRAY
                                                : rs.getBytes(i)),
                                        mandatoryEncoding);
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
                            if (metaData.getColumnTypeName(i).equalsIgnoreCase(
                                    "year")) {
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
                        //case Types.TIME_WITH_TIMEZONE:
                        //case Types.TIMESTAMP_WITH_TIMEZONE:
                        //    record.addColumn(new StringColumn(rs.getString(i)));
                        //    break;
                        case Types.ARRAY:
                            Object arrayObject = rs.getObject(i);
                            Array dataArray = (Array)arrayObject;
                            PostgisWrapper postgisWrapperForArray = new PostgisWrapper();
                            postgisWrapperForArray.setColumnTypeName(dataArray.getBaseTypeName());
                            postgisWrapperForArray.setRawData(dataArray.toString());
                            record.addColumn(new BytesColumn(JSON.toJSONBytes(postgisWrapperForArray)));
                            break;
                        case Types.OTHER:
                            Object object = rs.getObject(i);
                            if(Objects.nonNull(object)) {
                                String columnTypeName = metaData.getColumnTypeName(i);
                                PostgisWrapper postgisWrapper = new PostgisWrapper();
                                postgisWrapper.setColumnTypeName(columnTypeName);
                                if(object instanceof PGgeometry) {
                                    //PGgeometry can't be serialized directly, need to convert to WKT
                                    postgisWrapper.setRawData(object.toString());
                                } else if ( object instanceof PGobject){
                                    postgisWrapper.setRawData(JSON.toJSONString(object));
                                } else {
                                    throw new IllegalStateException("Unsupported PostGIS type: " + object.getClass().getName());
                                }
                                record.addColumn(
                                        new BytesColumn(JSON.toJSONBytes(postgisWrapper))
                                );
                            } else {
                                LOG.error("PostgisReader read column {} with type {} is null, please check your database data.",
                                        metaData.getColumnName(i), metaData.getColumnTypeName(i));
                                throw new IllegalStateException("can't be null value");
                            }
                            break;
                        default:
                            // warn:not support INTERVAL etc: Types.JAVA_OBJECT
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
                // TODO 这里识别为脏数据靠谱吗？
                taskPluginCollector.collectDirtyRecord(record, e);
                if (e instanceof DataXException) {
                    throw (DataXException) e;
                }
            }
            recordSender.sendToWriter(record);
            return record;
        }
    }
}
