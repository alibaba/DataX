package cn.com.bluemoon.metadata.base.config;

import cn.com.bluemoon.metadata.base.constant.Neo4jCypherConstants;
import com.alibaba.fastjson.JSONArray;
import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/config/CreateTypeConfigGenerator.class */
public class CreateTypeConfigGenerator {
    public static CreateTypeConfig generate(Map<String, Object> taskConfig) {
        CreateTypeConfig config = new CreateTypeConfig();
        JSONArray column = (JSONArray) taskConfig.get("column");
        List<String> columns = Lists.newArrayList();
        Iterator it = column.iterator();
        while (it.hasNext()) {
            columns.add(it.next().toString());
        }
        config.setColumns(columns);
        config.setCreateType(taskConfig.get("create.type").toString());
        config.setEnvCn(taskConfig.get("create.envCn").toString());
        config.setCatalogCN(taskConfig.get("create.catalogCN").toString());
        if (config.getCreateType().equals(Neo4jCypherConstants.CREATE_TYPE_ON_NODE)) {
            config.setMode(taskConfig.get("create.NODE.mode").toString());
            config.setLabel(taskConfig.get("create.NODE.label").toString());
            config.setNodeName(taskConfig.get("create.NODE.nodeName").toString());
            config.setCompareId(taskConfig.get("create.NODE.compare.id").toString());
            String collectTime = taskConfig.get("create.NODE.collectTime").toString();
            if (StringUtils.isNotEmpty(collectTime)) {
                config.setCollectTime(Long.valueOf(Long.parseLong(collectTime)));
            }
            config.setIncrementMode(taskConfig.get("create.NODE.incrementMode").toString());
        } else if (config.getCreateType().equals(Neo4jCypherConstants.CREATE_TYPE_ON_REL)) {
            config.setSrcLabel(taskConfig.get("create.RELATIONSHIP.src.label").toString());
            config.setTargetLabel(taskConfig.get("create.RELATIONSHIP.target.label").toString());
            config.setRelLabel(taskConfig.get("create.RELATIONSHIP.label").toString());
            config.setMode(taskConfig.get("create.RELATIONSHIP.mode").toString());
            config.setSrcDegreeDir(taskConfig.get("create.RELATIONSHIP.src.degree.dir").toString());
            config.setTargetDegreeDir(taskConfig.get("create.RELATIONSHIP.target.degree.dir").toString());
        }
        return config;
    }
}
