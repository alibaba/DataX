package com.alibaba.datax.plugin.writer.doriswriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.fastjson2.JSON;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DorisJsonCodec extends DorisBaseCodec implements DorisCodec {

    private static final long serialVersionUID = 1L;

    private final List<String> fieldNames;

    public DorisJsonCodec ( List<String> fieldNames) {
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
