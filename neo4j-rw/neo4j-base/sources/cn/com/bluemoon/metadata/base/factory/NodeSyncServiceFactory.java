package cn.com.bluemoon.metadata.base.factory;

import cn.com.bluemoon.metadata.base.service.Neo4jOperatorService;
import cn.com.bluemoon.metadata.base.service.NodeSyncService;
import cn.com.bluemoon.metadata.base.service.impl.CompareNodeSyncServiceImpl;
import cn.com.bluemoon.metadata.base.service.impl.CreateNodeSyncServiceImpl;
import cn.com.bluemoon.metadata.base.service.impl.MergeNodeSyncServiceImpl;
import cn.com.bluemoon.metadata.base.service.impl.Neo4jOperatorServiceImpl;
import com.google.common.collect.Lists;
import java.util.List;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/factory/NodeSyncServiceFactory.class */
public class NodeSyncServiceFactory {
    private static final List<NodeSyncService> syncServices = Lists.newArrayList();

    static {
        Neo4jOperatorService neo4jOperatorService = new Neo4jOperatorServiceImpl();
        syncServices.add(new CreateNodeSyncServiceImpl(neo4jOperatorService));
        syncServices.add(new MergeNodeSyncServiceImpl(neo4jOperatorService));
        syncServices.add(new CompareNodeSyncServiceImpl(neo4jOperatorService));
    }

    public static NodeSyncService getNodeSyncService(String mode) {
        for (NodeSyncService syncService : syncServices) {
            if (syncService.support(mode)) {
                return syncService;
            }
        }
        throw new RuntimeException("暂时不支持【" + mode + "】模式的同步！");
    }
}
