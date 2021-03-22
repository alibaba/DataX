package com.dorisdb.connector.datax.plugin.writer.doriswriter.row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.datax.common.element.Record;
import com.alibaba.fastjson.JSON;

public class DorisJsonSerializer extends DorisBaseSerializer implements DorisISerializer {

    private static final long serialVersionUID = 1L;
    
    private final List<String> fieldNames;

    public DorisJsonSerializer(List<String> fieldNames) {
        this.fieldNames = fieldNames;
    }

    @Override
    public String serialize(Record row) {
        if (null == fieldNames) {
            return "";
        }
        Map<String, Object> rowMap = new HashMap<>(fieldNames.size());
        int idx = 0;
        for (String fieldName : fieldNames) {
            rowMap.put(fieldName, fieldConvertion(row.getColumn(idx)));
            idx++;
        }
        return JSON.toJSONString(rowMap);
    }
    
}
