package cn.com.bluemoon.metadata.base.service.impl;

import cn.com.bluemoon.metadata.base.config.CreateTypeConfig;
import cn.com.bluemoon.metadata.base.constant.Neo4jCypherConstants;
import cn.com.bluemoon.metadata.base.service.ICypherGenerator;
import cn.com.bluemoon.metadata.base.util.CypherHelper;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/service/impl/NodeCypherGenerator.class */
public class NodeCypherGenerator implements ICypherGenerator {
    private Logger logger = LoggerFactory.getLogger(NodeCypherGenerator.class);

    @Override // cn.com.bluemoon.metadata.base.service.ICypherGenerator
    public String generateCreateCql(CreateTypeConfig context) {
        String label = context.getLabel();
        List<String> columns = context.getColumns();
        ArrayList newArrayList = Lists.newArrayList(Neo4jCypherConstants.BASE_NODE_COLUMNS);
        newArrayList.removeAll(columns);
        List<String> tempColumns = new ArrayList<>(columns);
        tempColumns.addAll(newArrayList);
        String cypherQl = (("UNWIND  $batches AS batch CREATE  (n:`Neo4jNodeBaseEntity`:`" + label + "`") + ")") + " set " + CypherHelper.generatePropertyMap(tempColumns, "n", "batch");
        this.logger.info("Generated cypher ql:{}", cypherQl);
        return cypherQl;
    }

    @Override // cn.com.bluemoon.metadata.base.service.ICypherGenerator
    public String generateUpdateCql(CreateTypeConfig context) {
        String cypherQl = (("UNWIND  $batches AS batch match  (n:`Neo4jNodeBaseEntity`:`" + context.getLabel() + "`)") + " where n.guid=batch.guid and n.moiIsNewestVersion = 1") + " set " + CypherHelper.generatePropertyMap(context.getColumns(), "n", "batch") + " return n";
        this.logger.info("Generated cypher ql:{}", cypherQl);
        return cypherQl;
    }
}
