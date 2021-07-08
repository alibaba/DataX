package cn.sensorsdata.datax.plugin.util;


import cn.sensorsdata.datax.plugin.Converter;
import cn.sensorsdata.datax.plugin.domain.DataConverter;
import cn.sensorsdata.datax.plugin.domain.SaColumnItem;

import java.util.List;
import java.util.Objects;

public class ConverterUtil {

    public static Object convert(String targetColumnName, Object value, SaColumnItem col) {
        List<DataConverter> dataConverters = col.getDataConverters();
        if (Objects.isNull(dataConverters) || dataConverters.isEmpty()) {
            return value;
        }
        for (DataConverter dataConverter : dataConverters) {
            Converter converter = dataConverter.getConverter();
            if (Objects.isNull(converter)) {
                continue;
            }
            value = converter.transform(targetColumnName, value, dataConverter.getParam());
        }
        return value;
    }
}
