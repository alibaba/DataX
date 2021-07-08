package cn.sensorsdata.datax.plugin.converter;

import cn.hutool.core.collection.ConcurrentHashSet;
import cn.sensorsdata.datax.plugin.Converter;
import cn.sensorsdata.datax.plugin.ConverterFactory;
import cn.sensorsdata.datax.plugin.domain.DataConverter;
import cn.sensorsdata.datax.plugin.util.NullUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class IfNull2DefaultConverter implements Converter {

    private Map<String, List<DataConverter>> cache = new ConcurrentHashMap<>();
    private Set<String> cacheTargetColumnName = new ConcurrentHashSet<>();

    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param) {
        if (NullUtil.isNullOrBlank(value)) {
            Object o = param.get("default");
            if (cacheTargetColumnName.contains(cacheTargetColumnName)) {
                List<DataConverter> dataConverters = cache.get(targetColumnName);
                if (Objects.isNull(dataConverters) || dataConverters.isEmpty()) {
                    return o;
                }
                return trans(targetColumnName, o);
            } else {
                JSONArray dataConvertersJsonArray = (JSONArray) param.get("dataConverters");
                if (Objects.isNull(dataConvertersJsonArray)) {
                    return o;
                }
                String dataConvertersJsonStr = dataConvertersJsonArray.toJSONString();
                List<DataConverter> dataConverters = JSONObject.parseArray(dataConvertersJsonStr, DataConverter.class);
                if (Objects.isNull(dataConverters) || dataConverters.isEmpty()) {
                    cacheTargetColumnName.add(targetColumnName);
                    return o;
                }
                dataConverters.forEach(con -> {
                    con.setConverter(ConverterFactory.converter(con.getType()));
                });
                cache.put(targetColumnName, dataConverters);
                cacheTargetColumnName.add(targetColumnName);
                return trans(targetColumnName, o);
            }
        }
        return value;
    }

    private Object trans(String targetColumnName, Object o) {
        Object v = o;
        List<DataConverter> dataConverters = cache.get(targetColumnName);
        for (DataConverter dataConverter : dataConverters) {
            Converter con = dataConverter.getConverter();
            if (Objects.isNull(con)) {
                continue;
            }
            v = con.transform(targetColumnName, v, dataConverter.getParam());
        }
        return v;
    }
}
