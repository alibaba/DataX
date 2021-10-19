package com.alibaba.datax.plugin.writer.hbase20xsqlwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HBase20xSQLWriterTask {
    private final static Logger LOG = LoggerFactory.getLogger(HBase20xSQLWriterTask.class);

    private Configuration configuration;
    private TaskPluginCollector taskPluginCollector;

    private Connection connection = null;
    private PreparedStatement pstmt = null;

    // 需要向hbsae写入的列的数量,即用户配置的column参数中列的个数。时间戳不包含在内
    private int numberOfColumnsToWrite;
    // 期待从源头表的Record中拿到多少列
    private int numberOfColumnsToRead;
    private int[] columnTypes;
    private List<String> columns;
    private String fullTableName;

    private NullModeType nullModeType;
    private int batchSize;

    public HBase20xSQLWriterTask(Configuration configuration) {
        // 这里仅解析配置，不访问远端集群，配置的合法性检查在writer的init过程中进行
        this.configuration = configuration;
    }

    public void startWriter(RecordReceiver lineReceiver, TaskPluginCollector taskPluginCollector) {
        this.taskPluginCollector = taskPluginCollector;

        try {
            // 准备阶段
            initialize();

            // 写入数据
            writeData(lineReceiver);

        } catch (Throwable e) {
            throw DataXException.asDataXException(HBase20xSQLWriterErrorCode.PUT_HBASE_ERROR, e);
        } finally {
            // 关闭jdbc连接
            HBase20xSQLHelper.closeJdbc(connection, pstmt, null);
        }

    }

    /**
     * 初始化JDBC操作对象及列类型
     * @throws SQLException
     */
    private void initialize() throws SQLException {
        if (connection == null) {
            connection = HBase20xSQLHelper.getJdbcConnection(configuration);
            connection.setAutoCommit(false);
        }
        nullModeType = NullModeType.getByTypeName(configuration.getString(Key.NULLMODE, Constant.DEFAULT_NULL_MODE));
        batchSize = configuration.getInt(Key.BATCHSIZE, Constant.DEFAULT_BATCH_ROW_COUNT);
        String schema = configuration.getString(Key.SCHEMA);
        String tableName = configuration.getNecessaryValue(Key.TABLE, HBase20xSQLWriterErrorCode.REQUIRED_VALUE);
        fullTableName = "\"" + tableName + "\"";
        if (schema != null && !schema.isEmpty()) {
            fullTableName = "\"" + schema + "\".\"" + tableName + "\"";
        }
        columns = configuration.getList(Key.COLUMN, String.class);
        if (pstmt == null) {
            // 一个Task的生命周期中只使用一个PreparedStatement对象
            pstmt = createPreparedStatement();
            columnTypes = getColumnSqlType();
        }
    }

    /**
     * 生成sql模板，并根据模板创建PreparedStatement
     */
    private PreparedStatement createPreparedStatement() throws SQLException {
        // 生成列名集合，列之间用逗号分隔： col1,col2,col3,...
        StringBuilder columnNamesBuilder = new StringBuilder();
        for (String col : columns) {
            // 列名使用双引号，则不自动转换为全大写，而是保留用户配置的大小写
            columnNamesBuilder.append("\"");
            columnNamesBuilder.append(col);
            columnNamesBuilder.append("\"");
            columnNamesBuilder.append(",");
        }
        // 移除末尾多余的逗号
        columnNamesBuilder.setLength(columnNamesBuilder.length() - 1);
        String columnNames = columnNamesBuilder.toString();
        numberOfColumnsToWrite = columns.size();
        numberOfColumnsToRead = numberOfColumnsToWrite;   // 开始的时候，要读的列数娱要写的列数相等

        // 生成UPSERT模板
        StringBuilder upsertBuilder =
                new StringBuilder("upsert into " + fullTableName + " (" + columnNames + " ) values (");
        for (int i = 0; i < numberOfColumnsToWrite; i++) {
            upsertBuilder.append("?,");
        }
        upsertBuilder.setLength(upsertBuilder.length() - 1);  // 移除末尾多余的逗号
        upsertBuilder.append(")");

        String sql = upsertBuilder.toString();
        PreparedStatement ps = connection.prepareStatement(sql);
        LOG.debug("SQL template generated: " + sql);
        return ps;
    }

    /**
     * 根据列名来从数据库元数据中获取这一列对应的SQL类型
     */
    private int[] getColumnSqlType() throws SQLException {
        int[] types = new int[numberOfColumnsToWrite];
        StringBuilder columnNamesBuilder = new StringBuilder();
        for (String columnName : columns) {
            columnNamesBuilder.append("\"").append(columnName).append("\",");
        }
        columnNamesBuilder.setLength(columnNamesBuilder.length() - 1);
        // 查询一条数据获取表meta信息
        String selectSql = "SELECT " + columnNamesBuilder + " FROM " + fullTableName + " LIMIT 1";
        Statement statement = null;
        try {
            statement = connection.createStatement();
            ResultSetMetaData meta = statement.executeQuery(selectSql).getMetaData();

            for (int i = 0; i < columns.size(); i++) {
                String name = columns.get(i);
                types[i] = meta.getColumnType(i + 1);
                LOG.debug("Column name : " + name + ", sql type = " + types[i] + " " + meta.getColumnTypeName(i + 1));
            }
        } catch (SQLException e) {
            throw DataXException.asDataXException(HBase20xSQLWriterErrorCode.GET_TABLE_COLUMNTYPE_ERROR,
                    "获取表" + fullTableName + "列类型失败，请检查配置和服务状态或者联系HBase管理员.", e);
        } finally {
            HBase20xSQLHelper.closeJdbc(null, statement, null);
        }

        return types;
    }

    /**
     * 从接收器中获取每条记录，写入Phoenix
     */
    private void writeData(RecordReceiver lineReceiver) throws SQLException {
        List<Record> buffer = new ArrayList<Record>(batchSize);
        Record record = null;
        while ((record = lineReceiver.getFromReader()) != null) {
            // 校验列数量是否符合预期
            if (record.getColumnNumber() != numberOfColumnsToRead) {
                throw DataXException.asDataXException(HBase20xSQLWriterErrorCode.ILLEGAL_VALUE,
                        "数据源给出的列数量[" + record.getColumnNumber() + "]与您配置中的列数量[" + numberOfColumnsToRead +
                                "]不同, 请检查您的配置 或者 联系 Hbase 管理员.");
            }

            buffer.add(record);
            if (buffer.size() > batchSize) {
                doBatchUpsert(buffer);
                buffer.clear();
            }
        }

        // 处理剩余的record
        if (!buffer.isEmpty()) {
            doBatchUpsert(buffer);
            buffer.clear();
        }
    }

    /**
     * 批量提交一组数据，如果失败，则尝试一行行提交，如果仍然失败，抛错给用户
     */
    private void doBatchUpsert(List<Record> records) throws SQLException {
        try {
            // 将所有record提交到connection缓存
            for (Record r : records) {
                setupStatement(r);
                pstmt.addBatch();
            }

            pstmt.executeBatch();
            // 将缓存的数据提交到phoenix
            connection.commit();
            pstmt.clearParameters();
            pstmt.clearBatch();

        } catch (SQLException e) {
            LOG.error("Failed batch committing " + records.size() + " records", e);

            // 批量提交失败，则一行行重试，以确定哪一行出错
            connection.rollback();
            HBase20xSQLHelper.closeJdbc(null, pstmt, null);
            connection.setAutoCommit(true);
            pstmt = createPreparedStatement();
            doSingleUpsert(records);
        } catch (Exception e) {
            throw DataXException.asDataXException(HBase20xSQLWriterErrorCode.PUT_HBASE_ERROR, e);
        }
    }

    /**
     * 单行提交，将出错的行记录到脏数据中。由脏数据收集模块判断任务是否继续
     */
    private void doSingleUpsert(List<Record> records) throws SQLException {
        int rowNumber = 0;
        for (Record r : records) {
            try {
                rowNumber ++;
                setupStatement(r);
                pstmt.executeUpdate();
            } catch (SQLException e) {
                //出错了，记录脏数据
                LOG.error("Failed writing to phoenix, rowNumber: " + rowNumber);
                this.taskPluginCollector.collectDirtyRecord(r, e);
            }
        }
    }

    private void setupStatement(Record record) throws SQLException {
        for (int i = 0; i < numberOfColumnsToWrite; i++) {
            Column col = record.getColumn(i);
            int sqlType = columnTypes[i];
            // PreparedStatement中的索引从1开始，所以用i+1
            setupColumn(i + 1, sqlType, col);
        }
    }

    private void setupColumn(int pos, int sqlType, Column col) throws SQLException {
        if (col.getRawData() != null) {
            switch (sqlType) {
                case Types.CHAR:
                case Types.VARCHAR:
                    pstmt.setString(pos, col.asString());
                    break;

                case Types.BINARY:
                case Types.VARBINARY:
                    pstmt.setBytes(pos, col.asBytes());
                    break;

                case Types.BOOLEAN:
                    pstmt.setBoolean(pos, col.asBoolean());
                    break;

                case Types.TINYINT:
                case Constant.TYPE_UNSIGNED_TINYINT:
                    pstmt.setByte(pos, col.asLong().byteValue());
                    break;

                case Types.SMALLINT:
                case Constant.TYPE_UNSIGNED_SMALLINT:
                    pstmt.setShort(pos, col.asLong().shortValue());
                    break;

                case Types.INTEGER:
                case Constant.TYPE_UNSIGNED_INTEGER:
                    pstmt.setInt(pos, col.asLong().intValue());
                    break;

                case Types.BIGINT:
                case Constant.TYPE_UNSIGNED_LONG:
                    pstmt.setLong(pos, col.asLong());
                    break;

                case Types.FLOAT:
                    pstmt.setFloat(pos, col.asDouble().floatValue());
                    break;

                case Types.DOUBLE:
                    pstmt.setDouble(pos, col.asDouble());
                    break;

                case Types.DECIMAL:
                    pstmt.setBigDecimal(pos, col.asBigDecimal());
                    break;

                case Types.DATE:
                case Constant.TYPE_UNSIGNED_DATE:
                    pstmt.setDate(pos, new Date(col.asDate().getTime()));
                    break;

                case Types.TIME:
                case Constant.TYPE_UNSIGNED_TIME:
                    pstmt.setTime(pos, new Time(col.asDate().getTime()));
                    break;

                case Types.TIMESTAMP:
                case Constant.TYPE_UNSIGNED_TIMESTAMP:
                    pstmt.setTimestamp(pos, new Timestamp(col.asDate().getTime()));
                    break;

                default:
                    throw DataXException.asDataXException(HBase20xSQLWriterErrorCode.ILLEGAL_VALUE,
                            "不支持您配置的列类型:" + sqlType + ", 请检查您的配置 或者 联系 Hbase 管理员.");
            }
        } else {
            // 没有值，按空值的配置情况处理
            switch (nullModeType){
                case Skip:
                    // 跳过空值，则不插入该列,
                    pstmt.setNull(pos, sqlType);
                    break;

                case Empty:
                    // 插入"空值"，请注意不同类型的空值不同
                    // 另外，对SQL来说，空值本身是有值的，这与直接操作HBASE Native API时的空值完全不同
                    pstmt.setObject(pos, getEmptyValue(sqlType));
                    break;

                default:
                    // nullMode的合法性在初始化配置的时候已经校验过，这里一定不会出错
                    throw DataXException.asDataXException(HBase20xSQLWriterErrorCode.ILLEGAL_VALUE,
                            "Hbasewriter 不支持该 nullMode 类型: " + nullModeType +
                                    ", 目前支持的 nullMode 类型是:" + Arrays.asList(NullModeType.values()));
            }
        }
    }

    /**
     * 根据类型获取"空值"
     * 值类型的空值都是0，bool是false，String是空字符串
     * @param sqlType sql数据类型，定义于{@link Types}
     */
    private Object getEmptyValue(int sqlType) {
        switch (sqlType) {
            case Types.VARCHAR:
                return "";

            case Types.BOOLEAN:
                return false;

            case Types.TINYINT:
            case Constant.TYPE_UNSIGNED_TINYINT:
                return (byte) 0;

            case Types.SMALLINT:
            case Constant.TYPE_UNSIGNED_SMALLINT:
                return (short) 0;

            case Types.INTEGER:
            case Constant.TYPE_UNSIGNED_INTEGER:
                return (int) 0;

            case Types.BIGINT:
            case Constant.TYPE_UNSIGNED_LONG:
                return (long) 0;

            case Types.FLOAT:
                return (float) 0.0;

            case Types.DOUBLE:
                return (double) 0.0;

            case Types.DECIMAL:
                return new BigDecimal(0);

            case Types.DATE:
            case Constant.TYPE_UNSIGNED_DATE:
                return new Date(0);

            case Types.TIME:
            case Constant.TYPE_UNSIGNED_TIME:
                return new Time(0);

            case Types.TIMESTAMP:
            case Constant.TYPE_UNSIGNED_TIMESTAMP:
                return new Timestamp(0);

            case Types.BINARY:
            case Types.VARBINARY:
                return new byte[0];

            default:
                throw DataXException.asDataXException(HBase20xSQLWriterErrorCode.ILLEGAL_VALUE,
                        "不支持您配置的列类型:" + sqlType + ", 请检查您的配置 或者 联系 Hbase 管理员.");
        }
    }
}
