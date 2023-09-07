package com.alibaba.datax.plugin.writer.mock;


import com.alibaba.datax.common.element.*;
import com.alibaba.datax.plugin.writer.neo4jwriter.element.PropertyType;
import com.alibaba.fastjson2.JSON;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class MockUtil {

    public static Column mockColumnByType(PropertyType type) {
        Random random = new Random();
        switch (type) {
            case SHORT:
                return new StringColumn("1");
            case BOOLEAN:
                return new BoolColumn(random.nextInt() % 2 == 0);
            case INTEGER:
            case LONG:
                return new LongColumn(random.nextInt(Integer.MAX_VALUE));
            case FLOAT:
            case DOUBLE:
                return new DoubleColumn(random.nextDouble());
            case NULL:
                return null;
            case BYTE_ARRAY:
                return new BytesColumn(new byte[]{(byte) (random.nextInt() % 2)});
            case LOCAL_DATE:
                return new StringColumn(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
            case MAP:
                return new StringColumn(JSON.toJSONString(propmap()));
            case STRING_ARRAY:
                return new StringColumn("[1,1,1,1,1,1,1]");
            default:
                return new StringColumn("randomStr" + random.nextInt(Integer.MAX_VALUE));
        }
    }

    public static Map<String, Object> propmap() {
        Map<String, Object> prop = new HashMap<>();
        prop.put("name", "neo4jWriter");
        prop.put("age", "1");
        return prop;
    }
}
