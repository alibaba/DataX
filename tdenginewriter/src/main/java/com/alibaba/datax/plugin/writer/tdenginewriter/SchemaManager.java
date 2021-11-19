package com.alibaba.datax.plugin.writer.tdenginewriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

public class SchemaManager {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaManager.class);

    private String stable; // 目标超表名
    private Map<String, String> fixedTagValue = new HashMap<>(); // 固定标签值 标签名 -> 标签值
    private Map<String, Integer> tagIndexMap = new HashMap<>(); // 动态标签值  标签名 -> 列索引
    private Map<String, Integer> fieldIndexMap = new HashMap<>(); // 字段名 -> 字段索引
    private String tsColName; // 时间戳列名
    private int tsColIndex = -1; // 时间戳列索引
    private List<String> fieldList = new ArrayList<>();
    private List<String> tagList = new ArrayList<>();
    private boolean canInferSchemaFromConfig = false;


    public SchemaManager() {
    }

    public SchemaManager(Properties properties) {
        getFromConfig(properties);
    }

    private String mapDataxType(Column.Type type) {
        switch (type) {
            case LONG:
                return "BIGINT";
            case DOUBLE:
                return "DOUBLE";
            case STRING:
                return "NCHAR(64)";
            case DATE:
                return "TIMESTAMP";
            case BOOL:
                return "BOOL";
            case BYTES:
                return "BINARY(64)";
            default:
                throw DataXException.asDataXException(TDengineWriterErrorCode.TYPE_ERROR, type.toString());
        }
    }

    public void setStable(String stable) {
        stable = stable;
    }

    public String getStable() {
        return stable;
    }

    private void getFromConfig(Properties properties) {
        stable = properties.getProperty(Key.STABLE);
        if (stable == null) {
            LOG.error("Config error: no stable");
            return;
        }
        for (Object key : properties.keySet()) {
            String k = (String) key;
            String v = properties.getProperty(k);

            String[] ps = k.split("\\.");
            if (ps.length == 1) {
                continue;
            }
            if (k.startsWith(Key.TAG_COLUMN)) {
                String tagName = ps[1];
                try {
                    Integer tagIndex = Integer.parseInt(v);
                    this.tagIndexMap.put(tagName, tagIndex);
                    tagList.add(tagName);
                } catch (NumberFormatException e) {
                    fixedTagValue.put(tagName, v);
                    tagList.add(tagName);
                }
            } else if (k.startsWith(Key.FIELD_COLUMN)) {
                String fieldName = ps[1];
                Integer fileIndex = Integer.parseInt(v);
                fieldIndexMap.put(fieldName, fileIndex);
            } else if (k.startsWith(Key.TIMESTAMP_COLUMN)) {
                tsColName = ps[1];
                tsColIndex = Integer.parseInt(v);
            }
        }
        List<String> sortedFieldName = fieldIndexMap.entrySet().stream().sorted((x, y) -> x.getValue().compareTo(y.getValue())).map(e -> e.getKey()).collect(Collectors.toList());
        fieldList.addAll(sortedFieldName); // 排序的目的是保证自动建表时列的顺序和输入数据的列的顺序保持一致
        canInferSchemaFromConfig = tsColIndex > -1 && !(fixedTagValue.isEmpty() && tagIndexMap.isEmpty()) && !fieldIndexMap.isEmpty();
        LOG.info("Config file parsed result：fixedTags=[{}] ,tags=[{}], fields=[{}], tsColName={}, tsIndex={}", String.join(",", fixedTagValue.keySet()), String.join(",", tagIndexMap.keySet()), String.join(",", fieldList), tsColName, tsColIndex);
    }

    public boolean shouldGuessSchema() {
        return !canInferSchemaFromConfig;
    }

    public boolean shouldCreateTable() {
        return canInferSchemaFromConfig;
    }

    public boolean configValid() {
        boolean valid = (tagList.size() > 0 && fieldList.size() > 0 && tsColIndex > -1) || (tagList.size() == 0 && fieldList.size() == 0 && tsColIndex == -1);
        if (!valid) {
            LOG.error("Config error: tagColumn, fieldColumn and timestampColumn must be present together or absent together.");
        }
        return valid;
    }

    /**
     * 通过执行`describe dbname.stable`命令，获取表的schema.<br/>
     * describe命名返回有4列内容，分布是：Field,Type,Length,Note<br/>
     *
     * @return 成功返回true，如果超表不存在或其他错误则返回false
     */
    public boolean getFromDB(Connection conn) {
        try {
            List<String> stables = getSTables(conn);
            if (!stables.contains(stable)) {
                LOG.error("super table {} not exist， fail to get schema from database.", stable);
                return false;
            }
        } catch (SQLException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
            return false;
        }
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("describe " + stable);
            int colIndex = 0;
            while (rs.next()) {
                String name = rs.getString(1);
                String type = rs.getString(2);
                String note = rs.getString(4);
                if ("TIMESTAMP".equals(type)) {
                    tsColName = name;
                    tsColIndex = colIndex;
                } else if ("TAG".equals(note)) {
                    tagIndexMap.put(name, colIndex);
                    tagList.add(name);
                } else {
                    fieldIndexMap.put(name, colIndex);
                    fieldList.add(name);
                }
                colIndex++;
            }
            LOG.info("table info：tags=[{}], fields=[{}], tsColName={}, tsIndex={}", String.join(",", tagIndexMap.keySet()), String.join(",", fieldList), tsColName, tsColIndex);
            return true;
        } catch (SQLException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    public static List<String> getSTables(Connection conn) throws SQLException {
        List<String> stables = new ArrayList<>();
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("show stables");
            while (rs.next()) {
                String name = rs.getString(1);
                stables.add(name);
            }
        }
        return stables;
    }

    public void createSTable(Connection conn, List<Column.Type> fieldTypes) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE STABLE IF NOT EXISTS ").append(stable).append("(");
        sb.append(tsColName).append(" ").append("TIMESTAMP,");
        for (int i = 0; i < fieldList.size(); ++i) {
            String fieldName = fieldList.get(i);
            Column.Type dxType = fieldTypes.get(i);
            sb.append(fieldName).append(' ');
            String tdType = mapDataxType(dxType);
            sb.append(tdType).append(',');
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(") TAGS(");
        for (String tagName : tagList) {
            sb.append(tagName).append(" NCHAR(64),");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")");
        String q = sb.toString();
        LOG.info("run sql：" + q);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(q);
        }
    }

    public String[] getTagValuesFromRecord(Record record) {
        String[] tagValues = new String[tagList.size()];
        for (int i = 0; i < tagList.size(); ++i) {
            if (fixedTagValue.containsKey(tagList.get(i))) {
                tagValues[i] = fixedTagValue.get(tagList.get(i));
            } else {
                int tagIndex = tagIndexMap.get(tagList.get(i));
                tagValues[i] = record.getColumn(tagIndex).asString();
            }
            if (tagValues[i] == null) {
                return null;
            }
        }
        return tagValues;
    }

    public boolean hasTimestamp(Record record) {
        Column column = record.getColumn(tsColIndex);
        if (column.getType() == Column.Type.DATE && column.asDate() != null) {
            return true;
        } else {
            return false;
        }
    }

    public Map<String, Integer> getFieldIndexMap() {
        return fieldIndexMap;
    }

    public List<String> getFieldList() {
        return fieldList;
    }

    public String getJoinedFieldNames() {
        return tsColName + ", " + String.join(", ", fieldList);
    }

    public int getTsColIndex() {
        return tsColIndex;
    }

    public String getTagValuesPlaceHolder() {
        return tagList.stream().map(x -> "?").collect(Collectors.joining(","));
    }

    public String getFieldValuesPlaceHolder() {
        return "?, " + fieldList.stream().map(x -> "?").collect(Collectors.joining(", "));
    }

    /**
     * 计算子表表名
     * <ol>
     * <li>将标签的value 组合成为如下的字符串: tag_value1!tag_value2!tag_value3。</li>
     * <li>计算该字符串的 MD5 散列值 "md5_val"。</li>
     * <li>"t_md5val"作为子表名。其中的 "t" 是固定的前缀。</li>
     * </ol>
     *
     * @param tagValues
     * @return
     */
    public String computeTableName(String[] tagValues) {
        String s = String.join("!", tagValues);
        return "t_" + DigestUtils.md5Hex(s);
    }

    public int getDynamicTagCount() {
        return tagIndexMap.size();
    }
}
