package cn.com.bluemoon.metadata.base.service.impl;

import cn.com.bluemoon.metadata.base.config.CreateTypeConfig;
import cn.com.bluemoon.metadata.base.constant.Neo4jCypherConstants;
import cn.com.bluemoon.metadata.base.service.Neo4jOperatorService;
import cn.com.bluemoon.metadata.base.service.RelationSyncService;
import cn.com.bluemoon.metadata.neo4j.utils.DateUtils;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/service/impl/RelationSyncServiceImpl.class */
public class RelationSyncServiceImpl implements RelationSyncService {
    private static final Logger log = LoggerFactory.getLogger(RelationSyncServiceImpl.class);
    private Neo4jOperatorService neo4jOperatorService;

    /* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/service/impl/RelationSyncServiceImpl$LoadingCacheHolder.class */
    public static final class LoadingCacheHolder {
        public static final Cache<String, String> relationCache = CacheBuilder.newBuilder().initialCapacity(100).maximumSize(10000).expireAfterWrite(10, TimeUnit.MINUTES).build();
    }

    public RelationSyncServiceImpl(Neo4jOperatorService neo4jOperatorService) {
        this.neo4jOperatorService = neo4jOperatorService;
    }

    @Override // cn.com.bluemoon.metadata.base.service.RelationSyncService
    public void batchSync(Driver driver, CreateTypeConfig createTypeConfig, List<Map<String, Object>> allRecs) {
        for (Map<String, Object> map : allRecs) {
            replaceEnvAndCatalog(createTypeConfig, map, "sourceGuid");
            replaceEnvAndCatalog(createTypeConfig, map, "targetGuid");
        }
        Map<String, String> guidToMoiDataId = getGuidToMoiDataId(driver, allRecs);
        Date currentDate = new Date();
        allRecs.forEach(map2 -> {
            map2.put("relationType", "DRT.Aggregation.Composition");
            map2.put("relationshipIsDelete", 0);
            map2.put("moiCreateDate", Long.valueOf(Long.parseLong(DateUtils.format(currentDate, "yyyyMMddHHmmss"))));
            map2.put("sourceDataId", guidToMoiDataId.get(map2.get("sourceGuid")));
            map2.put("targetDataId", guidToMoiDataId.get(map2.get("targetGuid")));
            map2.put("sourceGuid", null);
            map2.put("targetGuid", null);
        });
        String cypher = new RelCypherGenerator().generateCreateCql(createTypeConfig);
        Map<String, Object> param = new HashMap<>();
        param.put(Neo4jCypherConstants.DEFAULT_BATCH_KEY, allRecs);
        log.info("正在做关系同步....本次关系同步{}->{},同步关系结果:同步关系{}条,当前已同步{}条.", new Object[]{createTypeConfig.getSrcLabel(), createTypeConfig.getTargetLabel(), Integer.valueOf(allRecs.size()), createTypeConfig.getRelTotalCount()});
        this.neo4jOperatorService.run(driver, cypher, param);
    }

    private void replaceEnvAndCatalog(CreateTypeConfig createTypeConfig, Map<String, Object> map, String key) {
        String value = map.get(key).toString();
        if (value.contains("{{envCn}}")) {
            value = value.replaceAll("\\{\\{envCn\\}\\}", createTypeConfig.getEnvCn());
        }
        if (value.contains("{{catalogCn}}")) {
            value = value.replaceAll("\\{\\{catalogCn\\}\\}", createTypeConfig.getCatalogCN());
        }
        map.put(key, value);
    }

    private Map<String, String> getGuidToMoiDataId(Driver driver, List<Map<String, Object>> allRecs) {
        Set<String> guidSets = Sets.newHashSet();
        allRecs.stream().forEach(x -> {
            guidSets.add(x.get("sourceGuid").toString());
            guidSets.add(x.get("targetGuid").toString());
        });
        Set<String> notInCacheSets = Sets.newHashSet();
        Map<String, String> guid2MoiDataId = Maps.newHashMap();
        for (String guidSet : guidSets) {
            String ifPresent = (String) LoadingCacheHolder.relationCache.getIfPresent(guidSet);
            if (StringUtils.isNotEmpty(ifPresent)) {
                guid2MoiDataId.put(guidSet, ifPresent);
            } else {
                notInCacheSets.add(guidSet);
            }
        }
        for (Map<String, Object> map : this.neo4jOperatorService.queryDataByGuid(driver, new ArrayList(notInCacheSets))) {
            try {
                guid2MoiDataId.put(map.get("guid").toString(), map.get("moiDataId").toString());
                LoadingCacheHolder.relationCache.put(map.get("guid").toString(), map.get("moiDataId").toString());
            } catch (NullPointerException e) {
                log.info("出现空指针异常，结果是：{}", map);
            }
        }
        return guid2MoiDataId;
    }
}
