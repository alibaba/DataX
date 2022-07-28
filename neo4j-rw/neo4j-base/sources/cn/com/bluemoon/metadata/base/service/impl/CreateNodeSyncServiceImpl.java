package cn.com.bluemoon.metadata.base.service.impl;

import cn.com.bluemoon.metadata.base.config.CreateTypeConfig;
import cn.com.bluemoon.metadata.base.enums.ModeEnums;
import cn.com.bluemoon.metadata.base.service.Neo4jOperatorService;
import java.util.List;
import java.util.Map;
import org.neo4j.driver.Driver;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/service/impl/CreateNodeSyncServiceImpl.class */
public class CreateNodeSyncServiceImpl extends AbstractNodeSyncService {
    public CreateNodeSyncServiceImpl(Neo4jOperatorService neo4jOperatorService) {
        super(neo4jOperatorService);
    }

    @Override // cn.com.bluemoon.metadata.base.service.NodeSyncService
    public boolean support(String mode) {
        return ModeEnums.CREATE.getCode().equals(mode);
    }

    @Override // cn.com.bluemoon.metadata.base.service.NodeSyncService
    public void batchSync(Driver driver, CreateTypeConfig createTypeConfig, List<Map<String, Object>> allRecs) {
        addDefaultValue(createTypeConfig, allRecs);
        generatorDataId(driver, createTypeConfig, allRecs);
        this.neo4jOperatorService.batchInsert(driver, createTypeConfig, allRecs);
    }
}
