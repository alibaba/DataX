package cn.sensorsdata.datax.plugin.converter;

import cn.sensorsdata.datax.plugin.Converter;
import cn.sensorsdata.datax.plugin.util.NullUtil;

import java.util.Date;
import java.util.Map;

public class Date2LongConverter implements Converter {

    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param) {
        if (NullUtil.isNullOrBlank(value)) {
            return (Long) null;
        }
        Date date = (Date) value;
        return date.getTime();
    }
}
