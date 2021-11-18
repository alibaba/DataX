package com.alibaba.datax.plugin.writer.tdenginewriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.taosdata.jdbc.TSDBPreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 使用JDBC原生写入接口批量写入。<br/>
 * 有两个限制条件导致批量写入的代码逻辑过于复杂，以至于需要开发新的类来封装。<br/>
 * 1. 用户必须提前把需要批量写入的数据搜集到ArrayList中
 * 2. 每批写入的表名必须相同。
 * 这个类的实现逻辑是：
 * 1. 先把属于同一子表的Record缓存起来
 * 2. 缓存的数量达到batchSize阈值，自动执行一次批量写入
 * 3. 最后一批数据需要用户手动flush才能写入
 */
public class JDBCBatchWriter {
    public static final Logger LOG = LoggerFactory.getLogger(JDBCBatchWriter.class);

    private TSDBPreparedStatement stmt;
    private SchemaManager scm;
    private int batchSize;
    // 缓存Record， key为tableName
    Map<String, List<Record>> buf = new HashMap<>();
    // 缓存表的标签值， key为tableName
    Map<String, String[]> tableTagValues = new HashMap<>();
    private long sucCount = 0;
    private final int tsColIndex;
    private List<String> fieldList;
    private Map<String, Integer> fieldIndexMap;

    public JDBCBatchWriter(TSDBPreparedStatement stmt, SchemaManager scm, int batchSize) {
        this.stmt = stmt;
        this.scm = scm;
        this.batchSize = batchSize;
        this.tsColIndex = scm.getTsColIndex();
        this.fieldList = scm.getFieldList();
        this.fieldIndexMap = scm.getFieldIndexMap();
    }


    public void append(Record record) throws SQLException {
        String[] tagValues = scm.getTagValuesFromRecord(record);
        String tableName = scm.computeTableName(tagValues);
        if (buf.containsKey(tableName)) {
            List<Record> lis = buf.get(tableName);
            lis.add(record);
            if (lis.size() == batchSize) {
                executeBatch(tableName);
                lis.clear();
            }
        } else {
            List<Record> lis = new ArrayList<>(batchSize);
            lis.add(record);
            buf.put(tableName, lis);
            tableTagValues.put(tableName, tagValues);
        }
    }

    /**
     * 执行单表批量写入
     *
     * @param tableName
     * @throws SQLException
     */
    private void executeBatch(String tableName) throws SQLException {
        // 表名
        stmt.setTableName(tableName);
        List<Record> records = buf.get(tableName);
        // 标签
        String[] tagValues = tableTagValues.get(tableName);
        LOG.debug("executeBatch {}", String.join(",", tagValues));
        for (int i = 0; i < tagValues.length; ++i) {
            stmt.setTagNString(i, tagValues[i]);
        }
        // 时间戳
        ArrayList<Long> tsList = records.stream().map(r -> r.getColumn(tsColIndex).asDate().getTime()).collect(Collectors.toCollection(ArrayList::new));
        stmt.setTimestamp(0, tsList);
        // 字段
        Record record = records.get(0);
        for (int i = 0; i < fieldList.size(); ) {
            String fieldName = fieldList.get(i);
            int index = fieldIndexMap.get(fieldName);
            Column column = record.getColumn(index);
            switch (column.getType()) {
                case LONG:
                    ArrayList<Long> lisLong = records.stream().map(r -> r.getColumn(index).asBigInteger().longValue()).collect(Collectors.toCollection(ArrayList::new));
                    stmt.setLong(++i, lisLong);
                    break;
                case DOUBLE:
                    ArrayList<Double> lisDouble = records.stream().map(r -> r.getColumn(index).asDouble()).collect(Collectors.toCollection(ArrayList::new));
                    stmt.setDouble(++i, lisDouble);
                    break;
                case STRING:
                    ArrayList<String> lisString = records.stream().map(r -> r.getColumn(index).asString()).collect(Collectors.toCollection(ArrayList::new));
                    stmt.setNString(++i, lisString, 64);
                    break;
                case DATE:
                    ArrayList<Long> lisTs = records.stream().map(r -> r.getColumn(index).asBigInteger().longValue()).collect(Collectors.toCollection(ArrayList::new));
                    stmt.setTimestamp(++i, lisTs);
                    break;
                case BOOL:
                    ArrayList<Boolean> lisBool = records.stream().map(r -> r.getColumn(index).asBoolean()).collect(Collectors.toCollection(ArrayList::new));
                    stmt.setBoolean(++i, lisBool);
                    break;
                case BYTES:
                    ArrayList<String> lisBytes = records.stream().map(r -> r.getColumn(index).asString()).collect(Collectors.toCollection(ArrayList::new));
                    stmt.setString(++i, lisBytes, 64);
                    break;
                default:
                    throw DataXException.asDataXException(TDengineWriterErrorCode.TYPE_ERROR, column.getType().toString());
            }
        }
        // 执行
        stmt.columnDataAddBatch();
        stmt.columnDataExecuteBatch();
        // 更新计数器
        sucCount += records.size();
    }

    /**
     * 把缓存的Record全部写入
     */
    public void flush() throws SQLException {
        for (String tabName : buf.keySet()) {
            if (buf.get(tabName).size() > 0) {
                executeBatch(tabName);
            }
        }
        stmt.columnDataCloseBatch();
    }

    /**
     * @return 成功写入的数据量
     */
    public long getCount() {
        return sucCount;
    }
}
