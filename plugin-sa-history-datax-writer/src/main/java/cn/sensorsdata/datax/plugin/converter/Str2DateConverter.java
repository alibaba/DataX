package cn.sensorsdata.datax.plugin.converter;

import cn.hutool.core.util.StrUtil;
import cn.sensorsdata.datax.plugin.Converter;
import cn.sensorsdata.datax.plugin.util.DateUtil;
import cn.sensorsdata.datax.plugin.util.NullUtil;
import com.alibaba.fastjson.JSONArray;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Str2DateConverter implements Converter {

    private Set<String> formatsSet = new HashSet<>();

    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param) {
        if (NullUtil.isNullOrBlank(value)) {
            return (Date) null;
        }
        String pattern = (String) param.get("pattern");
        if (!formatsSet.contains(targetColumnName)) {
            JSONArray formatsJsonArray = (JSONArray) param.get("formats");
            if (!Objects.isNull(formatsJsonArray)) {
                formatsJsonArray.forEach(f -> {
                    if (!DateUtil.hasFormatCustomize(f.toString())) {
                        DateUtil.registerFormat(f.toString());
                    }
                });
            }
            formatsSet.add(targetColumnName);
        }
        if (StrUtil.isBlank(pattern)) {
            pattern = DateUtil.getPattern((String) value);
            if (StrUtil.isBlank(pattern)) {
                Date date = DateUtil.str2DateCustomize((String) value);
                return date;
            }
        }
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        try {
            return sdf.parse(value.toString());
        } catch (ParseException e) {
            return DateUtil.str2Date((String) value);
        }
    }
}
