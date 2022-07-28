package cn.com.bluemoon.metadata.base.executor;

import cn.com.bluemoon.metadata.base.config.CreateTypeConfig;
import cn.com.bluemoon.metadata.base.constant.Neo4jCypherConstants;
import cn.com.bluemoon.metadata.base.factory.NodeSyncServiceFactory;
import cn.com.bluemoon.metadata.base.factory.RelationSyncServiceFactory;
import java.util.List;
import java.util.Map;
import org.neo4j.driver.Driver;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/executor/Neo4jWriterExecutor.class */
public class Neo4jWriterExecutor {
    public static void batchExecute(Driver driver, CreateTypeConfig createTypeConfig, List<Map<String, Object>> allRecs) throws ClassNotFoundException {
        if (createTypeConfig.getCreateType().equals(Neo4jCypherConstants.CREATE_TYPE_ON_NODE)) {
            createTypeConfig.setNodeTotalCount(Integer.valueOf(createTypeConfig.getNodeTotalCount().intValue() + allRecs.size()));
            NodeSyncServiceFactory.getNodeSyncService(createTypeConfig.getMode()).batchSync(driver, createTypeConfig, allRecs);
        } else if (createTypeConfig.getCreateType().equals(Neo4jCypherConstants.CREATE_TYPE_ON_REL)) {
            createTypeConfig.setRelTotalCount(Integer.valueOf(createTypeConfig.getRelTotalCount().intValue() + allRecs.size()));
            RelationSyncServiceFactory.getRelationSyncService().batchSync(driver, createTypeConfig, allRecs);
        } else {
            throw new UnsupportedOperationException("现在不允许创建同步【" + createTypeConfig.getCreateType() + "】类型的作业！");
        }
    }
}
