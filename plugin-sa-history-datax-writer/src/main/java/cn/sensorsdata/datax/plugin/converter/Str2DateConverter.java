package cn.sensorsdata.datax.plugin.converter;

import cn.hutool.core.util.StrUtil;
import cn.sensorsdata.datax.plugin.Converter;
import cn.sensorsdata.datax.plugin.util.DateUtil;
import cn.sensorsdata.datax.plugin.util.NullUtil;
import com.alibaba.fastjson.JSONArray;
import lombok.extern.slf4j.Slf4j;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
public class Str2DateConverter implements Converter {

    private Set<String> formatsSet = new HashSet<>();

    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param) {
        if (NullUtil.isNullOrBlank(value)) {
            return (Date) null;
        }
        String pattern = null;
        try {
            pattern = (String) param.get("pattern");
        }catch (Exception e){}
        if (!formatsSet.contains(targetColumnName)) {
            JSONArray formatsJsonArray = null;
            try {
                formatsJsonArray = (JSONArray) param.get("formats");
            }catch (Exception e){}
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
