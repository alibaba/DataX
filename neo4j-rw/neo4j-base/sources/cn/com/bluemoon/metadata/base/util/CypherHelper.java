package cn.com.bluemoon.metadata.base.util;

import cn.com.bluemoon.metadata.base.constant.Neo4jCypherConstants;
import java.util.List;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/util/CypherHelper.class */
public class CypherHelper {
    public static String generatePropertyMap(List<String> columns, String n, String batch) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            sb.append(n).append('.').append(columns.get(i)).append("=").append(batch).append('.').append(columns.get(i));
            if (i != columns.size() - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        System.out.println(generatePropertyMap(Neo4jCypherConstants.BASE_RELATION_COLUMNS, "rel", "batch"));
    }
}
