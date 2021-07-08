package cn.sensorsdata.datax.plugin.converter;

import cn.hutool.core.util.StrUtil;
import cn.sensorsdata.datax.plugin.Converter;
import cn.sensorsdata.datax.plugin.util.NullUtil;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class Date2StrConverter implements Converter {
    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param) {
        if (NullUtil.isNullOrBlank(value)) {
            return (String) null;
        }
        Date date = (Date) value;
        String pattern = (String) param.get("pattern");
        if (StrUtil.isBlank(pattern)) {
            throw new RuntimeException("Date2Str转换器pattern参数不能为空");
        }
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.format(date);
    }
}
