package cn.com.bluemoon.metadata.base.util;

import cn.com.bluemoon.metadata.base.dto.DataSecuritySyncParam;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/util/Neo4jToRelationDbSyncParamUtils.class */
public class Neo4jToRelationDbSyncParamUtils {
    public static List<DataSecuritySyncParam> getDataSecuritySyncParam(String configPath) throws DocumentException {
        List<DataSecuritySyncParam> result = new ArrayList<>();
        for (Element childElement : new SAXReader().read(DbMetaDataSyncParamUtils.class.getResourceAsStream(configPath)).getRootElement().elements()) {
            String mapColumns = getAttribute(childElement, "mapColumns", configPath);
            String seq = getAttribute(childElement, "seq", configPath);
            String targetTable = getAttribute(childElement, "targetTable", configPath);
            String countCql = childElement.element("countCql").getText();
            String cql = childElement.element("cql").getText();
            List<String> postSqlList = new ArrayList<>();
            Element postSql = childElement.element("postSql");
            if (postSql != null) {
                for (Element element : postSql.elements("sql")) {
                    postSqlList.add(element.getText());
                }
            }
            result.add(new DataSecuritySyncParam(mapColumns, targetTable, countCql, cql, postSqlList, childElement.getName(), Integer.valueOf(Integer.parseInt(seq))));
        }
        return result;
    }

    private static String getAttribute(Element element, String field, String configPath) {
        String s = element.attributeValue(field);
        if (!StringUtils.isEmpty(s)) {
            return s;
        }
        throw new RuntimeException("配置文件" + configPath + "中的" + element.getName() + "配置中缺少了属性参数:[" + field + "],请查看!");
    }
}
