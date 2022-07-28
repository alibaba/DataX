package cn.com.bluemoon.metadata.base.util;

import org.apache.commons.lang3.StringUtils;
import org.dom4j.Element;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/util/DbMetaDataSyncParamUtils.class */
public class DbMetaDataSyncParamUtils {
    /* JADX WARN: Multi-variable type inference failed */
    /* JADX WARN: Type inference failed for: r18v0, types: [cn.com.bluemoon.metadata.base.dto.DbMetaDataBaseSyncParam, java.lang.Object] */
    /* JADX WARN: Type inference failed for: r18v2, types: [cn.com.bluemoon.metadata.base.dto.DbMetaDataNodeSyncParam] */
    /* JADX WARN: Type inference failed for: r18v3, types: [cn.com.bluemoon.metadata.base.dto.DbMetaDataRelationShipSyncParam] */
    /* JADX WARN: Unknown variable types count: 1 */
    /* Code decompiled incorrectly, please refer to instructions dump */
    public static java.util.List<cn.com.bluemoon.metadata.base.dto.DbMetaDataBaseSyncParam> getSyncParamByDatasourceType(java.lang.String r5) throws org.dom4j.DocumentException {
        /*
        // Method dump skipped, instructions count: 456
        */
        throw new UnsupportedOperationException("Method not decompiled: cn.com.bluemoon.metadata.base.util.DbMetaDataSyncParamUtils.getSyncParamByDatasourceType(java.lang.String):java.util.List");
    }

    private static String getAttribute(Element element, String field, String configPath) {
        String s = element.attributeValue(field);
        if (!StringUtils.isEmpty(s)) {
            return s;
        }
        throw new RuntimeException("配置文件" + configPath + "中的" + element.getName() + "配置中缺少了属性参数:[" + field + "],请查看!");
    }
}
