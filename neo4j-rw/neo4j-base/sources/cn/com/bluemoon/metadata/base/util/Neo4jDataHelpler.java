package cn.com.bluemoon.metadata.base.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.types.Node;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/util/Neo4jDataHelpler.class */
public class Neo4jDataHelpler {
    public static List<Map<String, Object>> queryNodeForList(String cypher, Session session, Map<String, Object> parameters) {
        List<Map<String, Object>> result = Lists.newArrayList();
        List<Record> list = session.run(cypher, parameters).list();
        if (CollectionUtils.isNotEmpty(list)) {
            for (Record record : list) {
                Map<String, Object> map = Maps.newHashMap();
                for (String key : record.keys()) {
                    Node node = record.get(key).asNode();
                    for (String s : node.keys()) {
                        map.put(s, node.get(s).asObject());
                    }
                    result.add(map);
                }
            }
        }
        return result;
    }
}
