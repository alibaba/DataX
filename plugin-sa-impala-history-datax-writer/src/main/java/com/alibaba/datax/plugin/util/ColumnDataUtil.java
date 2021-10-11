package com.alibaba.datax.plugin.util;

import com.alibaba.datax.plugin.domain.TableColumnMetaData;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public class ColumnDataUtil {

    private ColumnDataUtil(){}

    public static String transformInsertBatchSql(String tableName, List<String> tableColumnOrderList, Map<String, TableColumnMetaData> tableColumnMetaDataMap, List<Map<String,Object>> propertiesList){
        StringBuilder sb = new StringBuilder("INSERT INTO ").append(tableName).append("(")
                .append(tableColumnOrderList.stream().collect(Collectors.joining(","))).append(") VALUES ");
        StringBuilder sbTemp;
        for (Map<String, Object> properties : propertiesList) {
            sbTemp = new StringBuilder("(");
            int nullCount = 0;
            for (int i = 0; i < tableColumnOrderList.size(); i++) {
                String columnName = tableColumnOrderList.get(i);
                TableColumnMetaData tableColumnMetaData = tableColumnMetaDataMap.get(columnName);
                String columnValue = generateColumnValue(columnName,tableColumnMetaData,properties.get(columnName));
                if(Objects.isNull(columnValue) || "null".equalsIgnoreCase(columnValue)){
                    nullCount ++;
                }
                sbTemp.append(columnValue).append(",");
            }
            sbTemp.deleteCharAt(sbTemp.length() - 1);
            sbTemp.append("),");
            if(nullCount != tableColumnOrderList.size()){
                sb.append(sbTemp.toString());
            }
        }
        if(sb.toString().endsWith(") VALUES ")){
//            这里说明没有一个要插入的属性值
            return null;
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public static String transformInsertSql(String tableName, List<String> tableColumnOrderList, Map<String, TableColumnMetaData> tableColumnMetaDataMap, Map<String,Object> properties){
        StringBuilder sb = new StringBuilder("INSERT INTO ").append(tableName).append("(")
                .append(tableColumnOrderList.stream().collect(Collectors.joining(","))).append(") VALUES (");
        int nullCount = 0;
        for (int i = 0; i < tableColumnOrderList.size(); i++) {
            String columnName = tableColumnOrderList.get(i);
            TableColumnMetaData tableColumnMetaData = tableColumnMetaDataMap.get(columnName);
            String columnValue = generateColumnValue(columnName,tableColumnMetaData,properties.get(columnName));
            if(Objects.isNull(columnValue) || "null".equalsIgnoreCase(columnValue)){
                nullCount ++;
            }
            sb.append(columnValue).append(",");
        }
        if(nullCount == tableColumnOrderList.size()){
//            这里说明没有一个要插入的属性值
            return null;
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")");
        return sb.toString();
    }

    public static String transformUpdateSql(String tableName, List<String> tableColumnOrderList, Map<String, TableColumnMetaData> tableColumnMetaDataMap, List<String> updateWhereColumn, Map<String, Object> properties) {
        StringBuilder sb = new StringBuilder("UPDATE ").append(tableName).append(" SET ");
        for (String columnName : tableColumnOrderList) {
            if(updateWhereColumn.contains(columnName) || Objects.isNull(properties.get(columnName))
              || Objects.isNull(tableColumnMetaDataMap.get(columnName))){
                continue;
            }
            TableColumnMetaData tableColumnMetaData = tableColumnMetaDataMap.get(columnName);
            String columnValue = generateColumnValue(columnName, tableColumnMetaData, properties.get(columnName));
            sb.append(columnName).append(" = ").append(columnValue).append(",");
        }
        if(sb.toString().endsWith(" SET ")){
//            这里说明没有一个要修改的属性值
            return null;
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(" WHERE ");
        for (String columnName : updateWhereColumn) {
            Object value = properties.get(columnName);
            TableColumnMetaData tableColumnMetaData = tableColumnMetaDataMap.get(columnName);
            if(Objects.isNull(value) || Objects.isNull(tableColumnMetaData)){
                sb.append(columnName).append(" is null and ");
            }else{
                String columnValue = generateColumnValue(columnName, tableColumnMetaData, value);
                sb.append(columnName).append(" = ").append(columnValue).append(" and ");
            }
        }
        sb.delete(sb.length() - 4,sb.length());
        return sb.toString();
    }

    private static String generateColumnValue(String columnName, TableColumnMetaData tableColumnMetaData, Object value) {
        if(Objects.isNull(value) || Objects.isNull(tableColumnMetaData) || Objects.isNull(tableColumnMetaData.getType())
         || Objects.equals("",tableColumnMetaData.getType())){
            return "null";
        }
        String type = tableColumnMetaData.getType();
        if("string".equalsIgnoreCase(type) || "varchar".equalsIgnoreCase(type) || "char".equalsIgnoreCase(type)){
            return "\"" + value.toString() + "\"";
        }
        if("int".equalsIgnoreCase(type) || "bigint".equalsIgnoreCase(type) || "float".equalsIgnoreCase(type)
          || "integer".equalsIgnoreCase(type) || "double".equalsIgnoreCase(type) || "decimal".equalsIgnoreCase(type)
          || "tinyint".equalsIgnoreCase(type) || "smallint".equalsIgnoreCase(type) || "real".equalsIgnoreCase(type)){
            return value.toString();
        }
        if("boolean".equalsIgnoreCase(type)){
            return Boolean.parseBoolean(value.toString())+"";
        }
        if("timestamp".equalsIgnoreCase(type) || "datetime".equalsIgnoreCase(type)){
            Date d = DateUtil.toDate(value);
            if(Objects.isNull(d)){
                return "\"" + value.toString() + "\"";
            }
            return "\"" + DateUtil.date2Str(d) + "\"";
        }
        if("date".equalsIgnoreCase(type)){
            Date d = DateUtil.toDate(value);
            if(Objects.isNull(d)){
                return "\"" + value.toString() + "\"";
            }
            return "\"" + DateUtil.date2Str(d,"yyyy-MM-dd") + "\"";
        }
        log.info("暂不支持的数据类型，impala type:{},value:{},value type:{}",type,value,Objects.isNull(value)?"null":value.getClass());
        return "null";
    }

}
