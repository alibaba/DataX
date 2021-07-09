package cn.sensorsdata.datax.plugin.converter;

import cn.sensorsdata.datax.plugin.Converter;

import java.util.Map;

public class NotNull2NullConverter implements Converter {

    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param) {
        return null;
    }
}
