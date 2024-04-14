package com.alibaba.datax.plugin.reader.hdfsreader;

import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jitongchen
 * @date 2023/9/7 10:20 AM
 */
public class ParquetMessageHelper {
    public static Map<String, ParquetMeta> parseParquetTypes(List<org.apache.parquet.schema.Type> parquetTypes) {
        int fieldCount = parquetTypes.size();
        Map<String, ParquetMeta> parquetMetaMap = new HashMap<String, ParquetMeta>();
        for (int i = 0; i < fieldCount; i++) {
            org.apache.parquet.schema.Type type = parquetTypes.get(i);
            String name = type.getName();
            ParquetMeta parquetMeta = new ParquetMeta();
            parquetMeta.setName(name);
            OriginalType originalType = type.getOriginalType();
            parquetMeta.setOriginalType(originalType);
            if (type.isPrimitive()) {
                PrimitiveType primitiveType = type.asPrimitiveType();
                parquetMeta.setPrimitiveType(primitiveType);
            }
            parquetMetaMap.put(name, parquetMeta);
        }
        return parquetMetaMap;
    }
}
