package cn.com.bluemoon.metadata.base.factory;

import cn.com.bluemoon.metadata.base.service.RelationSyncService;
import cn.com.bluemoon.metadata.base.service.impl.Neo4jOperatorServiceImpl;
import cn.com.bluemoon.metadata.base.service.impl.RelationSyncServiceImpl;
import com.google.common.collect.Lists;
import java.util.List;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/factory/RelationSyncServiceFactory.class */
public class RelationSyncServiceFactory {
    private static final List<RelationSyncService> syncServices = Lists.newArrayList();

    static {
        syncServices.add(new RelationSyncServiceImpl(new Neo4jOperatorServiceImpl()));
    }

    public static RelationSyncService getRelationSyncService() {
        return syncServices.get(0);
    }
}
