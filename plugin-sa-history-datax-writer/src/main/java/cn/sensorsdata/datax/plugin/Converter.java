package cn.sensorsdata.datax.plugin;

import java.util.Map;

public interface Converter {

    Object transform(String targetColumnName, Object value, Map<String, Object> param);
}
