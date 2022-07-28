package cn.com.bluemoon.metadata.base.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/util/ReflectionUtils.class */
public class ReflectionUtils {
    public static List<Field> getFields(Class<?> clazz) {
        Field[] fields = clazz.getDeclaredFields();
        List<Field> list = new ArrayList<>();
        for (Field field : fields) {
            list.add(field);
        }
        return list;
    }
}
