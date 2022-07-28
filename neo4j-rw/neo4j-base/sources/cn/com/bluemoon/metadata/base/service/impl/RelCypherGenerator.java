package cn.com.bluemoon.metadata.base.service.impl;

import cn.com.bluemoon.metadata.base.config.CreateTypeConfig;
import cn.com.bluemoon.metadata.base.constant.Neo4jCypherConstants;
import cn.com.bluemoon.metadata.base.service.ICypherGenerator;
import cn.com.bluemoon.metadata.base.util.CypherHelper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/service/impl/RelCypherGenerator.class */
public class RelCypherGenerator implements ICypherGenerator {
    private Logger logger = LoggerFactory.getLogger(RelCypherGenerator.class);

    @Override // cn.com.bluemoon.metadata.base.service.ICypherGenerator
    public String generateCreateCql(CreateTypeConfig context) {
        String srcLabel = context.getSrcLabel();
        String srcDegreeDir = context.getSrcDegreeDir();
        String targetLabel = context.getTargetLabel();
        String targetDegreeDir = context.getTargetDegreeDir();
        String relLabel = context.getRelLabel();
        String mode = context.getMode();
        String body = " MATCH (s:`" + srcLabel + "`";
        if (StringUtils.isNotEmpty("moiDataId:#sourceDataId,moiIsNewestVersion:1")) {
            body = body + ("{" + "moiDataId:#sourceDataId,moiIsNewestVersion:1".replaceAll("#", "batch.") + "}");
        }
        String body2 = body + ") , (t:`" + targetLabel + "`";
        if (StringUtils.isNotEmpty("moiDataId:#targetDataId,moiIsNewestVersion:1")) {
            body2 = body2 + ("{" + "moiDataId:#targetDataId,moiIsNewestVersion:1".replaceAll("#", "batch.") + "}");
        }
        String body3 = (body2 + ")") + " " + mode + "(s)" + srcDegreeDir + "[rel:`" + relLabel + "`]" + targetDegreeDir + "(t)";
        if (CollectionUtils.isNotEmpty(Neo4jCypherConstants.BASE_RELATION_COLUMNS)) {
            body3 = body3 + " ON CREATE  SET " + CypherHelper.generatePropertyMap(Neo4jCypherConstants.BASE_RELATION_COLUMNS, "rel", "batch");
        }
        String cypherQl = "UNWIND  $batches AS batch " + body3;
        this.logger.info("Generated cypher ql:{}", cypherQl);
        return cypherQl;
    }

    @Override // cn.com.bluemoon.metadata.base.service.ICypherGenerator
    public String generateUpdateCql(CreateTypeConfig context) {
        return null;
    }
}
