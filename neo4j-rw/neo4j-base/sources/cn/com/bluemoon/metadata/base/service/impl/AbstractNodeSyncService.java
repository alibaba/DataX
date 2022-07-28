package cn.com.bluemoon.metadata.base.service.impl;

import cn.com.bluemoon.metadata.base.config.CreateTypeConfig;
import cn.com.bluemoon.metadata.base.constant.LabelConstants;
import cn.com.bluemoon.metadata.base.constant.Neo4jCypherConstants;
import cn.com.bluemoon.metadata.base.service.Neo4jOperatorService;
import cn.com.bluemoon.metadata.base.service.NodeSyncService;
import cn.com.bluemoon.metadata.base.util.JedisPoolUtil;
import cn.com.bluemoon.metadata.base.util.LabelEntityHolder;
import cn.com.bluemoon.metadata.base.util.ModelIdUtils;
import cn.com.bluemoon.metadata.neo4j.utils.DateUtils;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/service/impl/AbstractNodeSyncService.class */
public abstract class AbstractNodeSyncService implements NodeSyncService {
    protected static final Set<String> indexField = ImmutableSet.of("tableRows", "avgRowLength", "dataLength", "indexLength", "numFiles", "totalSize", new String[]{"lastModifiedTime", "lastAccessTime", "partitionCount"});
    protected Neo4jOperatorService neo4jOperatorService;
    private Logger logger = LoggerFactory.getLogger(AbstractNodeSyncService.class);
    protected final Set<String> dateFieldSet = ImmutableSet.of("moiCreateDate", "moiUpdateDate");
    protected final String[] LONG_DATA_KEY = {"columnId", "moiSeq"};

    public AbstractNodeSyncService(Neo4jOperatorService neo4jOperatorService) {
        this.neo4jOperatorService = neo4jOperatorService;
    }

    protected void addDefaultValue(CreateTypeConfig createTypeConfig, List<Map<String, Object>> allRecs) {
        List<Map<String, Object>> allRecs2 = (List) allRecs.stream().filter(x -> {
            return x.get("guid") != null;
        }).collect(Collectors.toList());
        Date currentDate = new Date();
        for (Map<String, Object> map : allRecs2) {
            map.put("moiIsDelete", 0);
            map.put("moiIsNewestVersion", 1);
            map.put("isSystem", "false");
            map.put("moiDataVersion", 1);
            if (createTypeConfig.getCollectTime() != null) {
                map.put("collectTime", createTypeConfig.getCollectTime());
            }
            for (String idxField : indexField) {
                if (map.get(idxField) != null) {
                    map.put(idxField, Long.valueOf(Long.parseLong(map.get(idxField).toString())));
                } else {
                    map.put(idxField, 0);
                }
            }
            for (String dataField : this.dateFieldSet) {
                if (map.get(dataField) != null) {
                    map.put(dataField, Long.valueOf(Long.parseLong(map.get(dataField).toString())));
                } else {
                    map.put(dataField, Long.valueOf(Long.parseLong(DateUtils.format(currentDate, "yyyyMMddHHmmss"))));
                }
            }
            if (map.get("moiSeq") == null) {
                map.put("moiSeq", -1);
            }
            map.put("moiClassName", LabelEntityHolder.getEntityClassNameByLabel(createTypeConfig.getLabel()));
            Set<String> longField = Sets.newHashSet(this.LONG_DATA_KEY);
            longField.retainAll(map.keySet());
            for (String s : longField) {
                if (map.get(s) != null) {
                    map.put(s, Long.valueOf(Long.parseLong(map.get(s).toString())));
                }
            }
            if (map.get("guid") != null) {
                String guid = map.get("guid").toString();
                if (guid.contains("{{envCn}}")) {
                    guid = guid.replaceAll("\\{\\{envCn\\}\\}", createTypeConfig.getEnvCn());
                }
                if (guid.contains("{{catalogCn}}")) {
                    guid = guid.replaceAll("\\{\\{catalogCn\\}\\}", createTypeConfig.getCatalogCN());
                }
                map.put("guid", guid);
            }
        }
    }

    public void addMoiParentDataId(Driver driver, CreateTypeConfig createTypeConfig, List<Map<String, Object>> allRecs) {
        Map<String, Map<String, Object>> parentInfoMap = getParentInfoMap(driver, allRecs);
        for (Map<String, Object> allRec : allRecs) {
            String guid = allRec.get("guid").toString();
            Map<String, Object> parentInfo = parentInfoMap.get(guid.contains(Neo4jCypherConstants.GUID_DELIMITER) ? guid.substring(0, guid.lastIndexOf(Neo4jCypherConstants.GUID_DELIMITER)) : guid);
            if (parentInfo != null) {
                allRec.put("moiParentDataId", parentInfo.get("moiDataId"));
                allRec.put("moiParentDataIdPath", parentInfo.get("moiDataIdPath"));
            } else {
                this.logger.error("数据异常！guid={}的父节点未找到", guid);
            }
        }
    }

    public void generatorDataId(Driver driver, CreateTypeConfig createTypeConfig, List<Map<String, Object>> allRecs) {
        Map<String, Map<String, Object>> parentInfoMap = getParentInfoMap(driver, allRecs);
        Jedis jedis = null;
        try {
            jedis = JedisPoolUtil.getJedis();
            for (Map<String, Object> allRec : allRecs) {
                String moiDataId = generatorMoiDataId(createTypeConfig, jedis);
                allRec.put("moiDataId", moiDataId);
                String guid = allRec.get("guid").toString();
                Map<String, Object> parentInfo = parentInfoMap.get(guid.contains(Neo4jCypherConstants.GUID_DELIMITER) ? guid.substring(0, guid.lastIndexOf(Neo4jCypherConstants.GUID_DELIMITER)) : guid);
                if (parentInfo != null) {
                    allRec.put("moiParentDataId", parentInfo.get("moiDataId"));
                    allRec.put("moiDataIdPath", parentInfo.get("moiDataIdPath") + Neo4jCypherConstants.GUID_DELIMITER + moiDataId);
                    allRec.put("moiParentDataIdPath", parentInfo.get("moiDataIdPath"));
                } else {
                    this.logger.error("数据异常！guid={}的父节点未找到", guid);
                }
            }
            if (jedis != null) {
                jedis.close();
            }
        } catch (Throwable th) {
            if (jedis != null) {
                jedis.close();
            }
            throw th;
        }
    }

    private Map<String, Map<String, Object>> getParentInfoMap(Driver driver, List<Map<String, Object>> allRecs) {
        Map<String, Map<String, Object>> parentInfoMap = Maps.newHashMap();
        Set<String> notInCache = Sets.newHashSet();
        for (String s : (Set) allRecs.stream().map(x -> {
            String guid = x.get("guid").toString();
            if (!guid.contains(Neo4jCypherConstants.GUID_DELIMITER)) {
                return guid;
            }
            return guid.substring(0, guid.lastIndexOf(Neo4jCypherConstants.GUID_DELIMITER));
        }).collect(Collectors.toSet())) {
            Map<String, Object> node = (Map) LoadingCacheHolder.nodeCache.getIfPresent(s);
            if (node == null) {
                notInCache.add(s);
            }
            parentInfoMap.put(s, node);
        }
        for (Map<String, Object> map : this.neo4jOperatorService.queryDataByGuid(driver, new ArrayList(notInCache))) {
            parentInfoMap.put(map.get("guid").toString(), map);
            LoadingCacheHolder.nodeCache.put(map.get("guid").toString(), map);
        }
        return parentInfoMap;
    }

    /* JADX INFO: Access modifiers changed from: private */
    /* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/service/impl/AbstractNodeSyncService$LoadingCacheHolder.class */
    public static final class LoadingCacheHolder {
        private static final Cache<String, Map<String, Object>> nodeCache = CacheBuilder.newBuilder().initialCapacity(100).maximumSize(10000).expireAfterWrite(10, TimeUnit.MINUTES).build();

        private LoadingCacheHolder() {
        }
    }

    private String generatorMoiDataId(CreateTypeConfig createTypeConfig, Jedis jedis) {
        String label = createTypeConfig.getLabel();
        String modelMoiDataIdPrefix = ModelIdUtils.getModelMoiDataIdPrefix(label);
        if (StringUtils.isEmpty(modelMoiDataIdPrefix)) {
            throw new RuntimeException("该模型对应的ID前缀不存在，请进行配置！");
        }
        return modelMoiDataIdPrefix + "=" + jedis.incr(LabelConstants.MODEL_ID_PREFIX + label);
    }
}
