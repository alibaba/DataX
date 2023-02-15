package com.alibaba.datax.plugin.writer.selectdbwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.fastjson2.JSON;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SelectdbJsonCodec extends SelectdbBaseCodec implements SelectdbCodec {

    private static final long serialVersionUID = 1L;

    private final List<String> fieldNames;

    public SelectdbJsonCodec ( List<String> fieldNames) {
        this.fieldNames = fieldNames;
    }

    @Override
    public String codec( Record row) {
        if (null == fieldNames) {
            return "";
        }
        Map<String, Object> rowMap = new HashMap<> (fieldNames.size());
        int idx = 0;
        for (String fieldName : fieldNames) {
            rowMap.put(fieldName, convertionField(row.getColumn(idx)));
            idx++;
        }
        return JSON.toJSONString(rowMap);
    }
}
