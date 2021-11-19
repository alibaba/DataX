package com.alibaba.datax.plugin.writer.tdenginewriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.taosdata.jdbc.TSDBPreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
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
    private Connection conn;
    private int batchSize;
    private TaskPluginCollector collector;

    // 缓存Record， key为tableName
    Map<String, List<Record>> buf = new HashMap<>();
    // 缓存表的标签值， key为tableName
    Map<String, String[]> tableTagValues = new HashMap<>();
    private long sucCount = 0;
    private final int tsColIndex;
    private List<String> fieldList;
    // 每个record至少应该包含的列数，用于校验数据
    private int minColNum = 0;
    private Map<String, Integer> fieldIndexMap;
    private List<Column.Type> fieldTypes = null;

    public JDBCBatchWriter(Connection conn, TSDBPreparedStatement stmt, SchemaManager scm, int batchSize, TaskPluginCollector collector) {
        this.conn = conn;
        this.stmt = stmt;
        this.scm = scm;
        this.batchSize = batchSize;
        this.collector = collector;
        this.tsColIndex = scm.getTsColIndex();
        this.fieldList = scm.getFieldList();
        this.fieldIndexMap = scm.getFieldIndexMap();
        this.minColNum = 1 + fieldList.size() + scm.getDynamicTagCount();

    }

    public void initFiledTypesAndTargetTable(List<Record> records) throws SQLException {
        if (fieldTypes != null) {
            return;
        }
        guessFieldTypes(records);
        if (scm.shouldCreateTable()) {
            scm.createSTable(conn, fieldTypes);
        }
    }

    public void append(Record record) throws SQLException {
        int columnNum = record.getColumnNumber();
        if (columnNum < minColNum) {
            // 实际列数小于期望列数
            collector.collectDirtyRecord(record, Msg.get("column_number_error"));
            return;
        }
        String[] tagValues = scm.getTagValuesFromRecord(record);
        if (tagValues == null) {
            // 标签列包含null
            collector.collectDirtyRecord(record, Msg.get("tag_value_error"));
            return;
        }
        if (!scm.hasTimestamp(record)) {
            // 时间戳列为null或类型错误
            collector.collectDirtyRecord(record, Msg.get("ts_value_error"));
            return;
        }
        String tableName = scm.computeTableName(tagValues);
        if (buf.containsKey(tableName)) {
            List<Record> lis = buf.get(tableName);
            lis.add(record);
            if (lis.size() == batchSize) {
                if (fieldTypes == null) {
                    initFiledTypesAndTargetTable(lis);
                }
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
     * 只有String类型比较特别，测试发现值为null的列会转成String类型。所以Column的类型为String并不代表这一列的类型真的是String。
     *
     * @param records
     */
    private void guessFieldTypes(List<Record> records) {
        fieldTypes = new ArrayList<>(fieldList.size());
        for (int i = 0; i < fieldList.size(); ++i) {
            int colIndex = fieldIndexMap.get(fieldList.get(i));
            boolean ok = false;
            for (int j = 0; j < records.size() && !ok; ++j) {
                Column column = records.get(j).getColumn(colIndex);
                Column.Type type = column.getType();
                switch (type) {
                    case LONG:
                    case DOUBLE:
                    case DATE:
                    case BOOL:
                    case BYTES:
                        if (column.getRawData() != null) {
                            fieldTypes.add(type);
                            ok = true;
                        }
                        break;
                    case STRING:
                        // 只有非null且非空的String列，才会被真的当作String类型。
                        String value = column.asString();
                        if (value != null && !"".equals(value)) {
                            fieldTypes.add(type);
                            ok = true;
                        }
                        break;
                    default:
                        throw DataXException.asDataXException(TDengineWriterErrorCode.TYPE_ERROR, fieldTypes.get(i).toString());
                }
            }
            if (!ok) {
                // 根据采样的%d条数据，无法推断第%d列的数据类型
                throw DataXException.asDataXException(TDengineWriterErrorCode.TYPE_ERROR, String.format(Msg.get("infer_column_type_error"), records.size(), i + 1));
            }
        }
        LOG.info("Field Types: {}", fieldTypes);
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
        for (int i = 0; i < fieldList.size(); ) {
            String fieldName = fieldList.get(i);
            int index = fieldIndexMap.get(fieldName);
            switch (fieldTypes.get(i)) {
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
                    throw DataXException.asDataXException(TDengineWriterErrorCode.TYPE_ERROR, fieldTypes.get(i).toString());
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
        if (fieldTypes == null) {
            List<Record> records = new ArrayList<>();
            for (List<Record> lis : buf.values()) {
                records.addAll(lis);
                if (records.size() > 100) {
                    break;
                }
            }
            if (records.size() > 0) {
                initFiledTypesAndTargetTable(records);
            } else {
                return;
            }
        }
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
