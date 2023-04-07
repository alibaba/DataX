package com.starrocks.connector.datax.plugin.writer.starrockswriter.row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.datax.common.element.Record;
import com.alibaba.fastjson2.JSON;

public class StarRocksJsonSerializer extends StarRocksBaseSerializer implements StarRocksISerializer {

    private static final long serialVersionUID = 1L;
    
    private final List<String> fieldNames;

    public StarRocksJsonSerializer(List<String> fieldNames) {
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
