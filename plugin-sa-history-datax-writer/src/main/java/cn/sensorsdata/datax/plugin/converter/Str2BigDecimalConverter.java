package cn.sensorsdata.datax.plugin.converter;

import cn.sensorsdata.datax.plugin.Converter;
import cn.sensorsdata.datax.plugin.util.NullUtil;

import java.math.BigDecimal;
import java.util.Map;

public class Str2BigDecimalConverter implements Converter {
    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param) {
        if (NullUtil.isNullOrBlank(value)) {
            return (BigDecimal) null;
        }
        return new BigDecimal(value.toString());
    }
}
