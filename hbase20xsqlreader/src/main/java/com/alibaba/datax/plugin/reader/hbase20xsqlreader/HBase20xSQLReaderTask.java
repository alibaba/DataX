package com.alibaba.datax.plugin.reader.hbase20xsqlreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;

public class HBase20xSQLReaderTask {
    private static final Logger LOG = LoggerFactory.getLogger(HBase20xSQLReaderTask.class);

    private Configuration readerConfig;
    private int taskGroupId = -1;
    private int taskId=-1;

    public HBase20xSQLReaderTask(Configuration config, int taskGroupId, int taskId) {
        this.readerConfig = config;
        this.taskGroupId = taskGroupId;
        this.taskId = taskId;
    }

    public void readRecord(RecordSender recordSender) {
        String querySql = readerConfig.getString(Constant.QUERY_SQL_PER_SPLIT);
        LOG.info("Begin to read record by Sql: [{}\n] {}.", querySql);
        HBase20SQLReaderHelper helper = new HBase20SQLReaderHelper(readerConfig);
        Connection conn = helper.getConnection(readerConfig.getString(Key.QUERYSERVER_ADDRESS),
                readerConfig.getString(Key.SERIALIZATION_NAME, Constant.DEFAULT_SERIALIZATION));
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            long rsNextUsedTime = 0;
            long lastTime = System.nanoTime();
            statement = conn.createStatement();
            // 统计查询时间
            PerfRecord queryPerfRecord = new PerfRecord(taskGroupId,taskId, PerfRecord.PHASE.SQL_QUERY);
            queryPerfRecord.start();

            resultSet = statement.executeQuery(querySql);
            ResultSetMetaData meta = resultSet.getMetaData();
            int columnNum = meta.getColumnCount();
            // 统计的result_Next时间
            PerfRecord allResultPerfRecord = new PerfRecord(taskGroupId, taskId, PerfRecord.PHASE.RESULT_NEXT_ALL);
            allResultPerfRecord.start();

            while (resultSet.next()) {
                Record record = recordSender.createRecord();
                rsNextUsedTime += (System.nanoTime() - lastTime);
                for (int i = 1; i <= columnNum; i++) {
                    Column column = this.convertPhoenixValueToDataxColumn(meta.getColumnType(i), resultSet.getObject(i));
                    record.addColumn(column);
                }
                lastTime = System.nanoTime();
                recordSender.sendToWriter(record);
            }
            allResultPerfRecord.end(rsNextUsedTime);
            LOG.info("Finished read record by Sql: [{}\n] {}.", querySql);
        } catch (SQLException e) {
            throw DataXException.asDataXException(
                    HBase20xSQLReaderErrorCode.QUERY_DATA_ERROR, "查询Phoenix数据出现异常，请检查服务状态或与HBase管理员联系！", e);
        } finally {
            helper.closeJdbc(conn, statement, resultSet);
        }

    }

    private Column convertPhoenixValueToDataxColumn(int sqlType, Object value) {
        Column column;
        switch (sqlType) {
            case Types.CHAR:
            case Types.VARCHAR:
                column = new StringColumn((String) value);
                break;
            case Types.BINARY:
            case Types.VARBINARY:
                column = new BytesColumn((byte[]) value);
                break;
            case Types.BOOLEAN:
                column = new BoolColumn((Boolean) value);
                break;
            case Types.INTEGER:
                column = new LongColumn((Integer) value);
                break;
            case Types.TINYINT:
                Byte aByte = (Byte) value;
                column = new LongColumn(null == aByte ? null : aByte.longValue());
                break;
            case Types.SMALLINT:
                Short aShort = (Short) value;
                column = new LongColumn(null == aShort ? null : aShort.longValue());
                break;
            case Types.BIGINT:
                column = new LongColumn((Long) value);
                break;
            case Types.FLOAT:
                column = new DoubleColumn(null == value ? null : (Float.valueOf(value.toString())));
                break;
            case Types.DECIMAL:
                column = new DoubleColumn((BigDecimal)value);
                break;
            case Types.DOUBLE:
                column = new DoubleColumn((Double) value);
                break;
            case Types.DATE:
                column = new DateColumn((Date) value);
                break;
            case Types.TIME:
                column = new DateColumn((Time) value);
                break;
            case Types.TIMESTAMP:
                column = new DateColumn((Timestamp) value);
                break;
            default:
                throw DataXException.asDataXException(
                        HBase20xSQLReaderErrorCode.PHOENIX_COLUMN_TYPE_CONVERT_ERROR, "遇到不可识别的phoenix类型，" + "sqlType :" + sqlType);
        }
        return column;
    }
}
