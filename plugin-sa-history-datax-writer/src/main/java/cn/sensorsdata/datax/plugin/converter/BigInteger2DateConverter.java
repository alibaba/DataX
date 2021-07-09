package cn.sensorsdata.datax.plugin.converter;

import cn.sensorsdata.datax.plugin.Converter;
import cn.sensorsdata.datax.plugin.util.NullUtil;

import java.math.BigInteger;
import java.util.Date;
import java.util.Map;

public class BigInteger2DateConverter implements Converter {
    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param) {
        if (NullUtil.isNullOrBlank(value)) {
            return (Date) null;
        }
        BigInteger v = (BigInteger) value;
        return new Date(v.longValue());
    }
}
