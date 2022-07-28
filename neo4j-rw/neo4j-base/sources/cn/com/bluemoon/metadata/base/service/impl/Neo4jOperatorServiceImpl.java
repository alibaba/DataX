package cn.com.bluemoon.metadata.base.service.impl;

import cn.com.bluemoon.metadata.base.config.CreateTypeConfig;
import cn.com.bluemoon.metadata.base.constant.Neo4jCypherConstants;
import cn.com.bluemoon.metadata.base.service.Neo4jOperatorService;
import cn.com.bluemoon.metadata.base.util.Neo4jDataHelpler;
import cn.com.bluemoon.metadata.common.enums.CollectTypeEnum;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/service/impl/Neo4jOperatorServiceImpl.class */
public class Neo4jOperatorServiceImpl implements Neo4jOperatorService {
    private final String DELETE_NODE_REL = "match (n:Neo4jNodeBaseEntity)-[r]-(l:Neo4jNodeBaseEntity) where n.guid in {guids} delete r";
    private final String DELETE_NODE = "match (n:Neo4jNodeBaseEntity) where n.guid in {guids} delete n";

    @Override // cn.com.bluemoon.metadata.base.service.Neo4jOperatorService
    public void run(Driver driver, String cypher, Map<String, Object> param) {
        Session session = driver.session();
        Throwable th = null;
        try {
            session.run(cypher, param);
            if (session == null) {
                return;
            }
            if (0 != 0) {
                try {
                    session.close();
                } catch (Throwable th2) {
                    th.addSuppressed(th2);
                }
            } else {
                session.close();
            }
        } catch (Throwable th3) {
            if (session != null) {
                if (0 != 0) {
                    try {
                        session.close();
                    } catch (Throwable th4) {
                        th.addSuppressed(th4);
                    }
                } else {
                    session.close();
                }
            }
            throw th3;
        }
    }

    @Override // cn.com.bluemoon.metadata.base.service.Neo4jOperatorService
    public void batchInsert(Driver driver, CreateTypeConfig createTypeConfig, List<Map<String, Object>> allRecs) {
        String cypher = new NodeCypherGenerator().generateCreateCql(createTypeConfig);
        Map<String, Object> param = Maps.newHashMap();
        param.put(Neo4jCypherConstants.DEFAULT_BATCH_KEY, allRecs);
        Session session = driver.session();
        Throwable th = null;
        try {
            session.run(cypher, param);
            if (session == null) {
                return;
            }
            if (0 != 0) {
                try {
                    session.close();
                } catch (Throwable th2) {
                    th.addSuppressed(th2);
                }
            } else {
                session.close();
            }
        } catch (Throwable th3) {
            if (session != null) {
                if (0 != 0) {
                    try {
                        session.close();
                    } catch (Throwable th4) {
                        th.addSuppressed(th4);
                    }
                } else {
                    session.close();
                }
            }
            throw th3;
        }
    }

    @Override // cn.com.bluemoon.metadata.base.service.Neo4jOperatorService
    public void resetNewestToZero(Driver driver, List<String> moiDataIds) {
        if (!moiDataIds.isEmpty()) {
            Map<String, Object> param = Maps.newHashMap();
            param.put("moiDataIds", moiDataIds);
            Session session = driver.session();
            Throwable th = null;
            try {
                session.run("match (n:Neo4jNodeBaseEntity) where n.moiDataId in {moiDataIds} set n.moiIsNewestVersion=0 , n.moiIsDelete = 1 return count(n)", param);
                if (session == null) {
                    return;
                }
                if (0 != 0) {
                    try {
                        session.close();
                    } catch (Throwable th2) {
                        th.addSuppressed(th2);
                    }
                } else {
                    session.close();
                }
            } catch (Throwable th3) {
                if (session != null) {
                    if (0 != 0) {
                        try {
                            session.close();
                        } catch (Throwable th4) {
                            th.addSuppressed(th4);
                        }
                    } else {
                        session.close();
                    }
                }
                throw th3;
            }
        }
    }

    @Override // cn.com.bluemoon.metadata.base.service.Neo4jOperatorService
    public List<Map<String, Object>> queryDataByMoiDataIds(Driver driver, List<String> moiDataIds) {
        Map<String, Object> param = Maps.newHashMap();
        param.put("moiDataIds", moiDataIds);
        Session session = driver.session();
        Throwable th = null;
        try {
            List<Map<String, Object>> existing = Neo4jDataHelpler.queryNodeForList("match (n:Neo4jNodeBaseEntity) where n.moiIsDelete=0 and n.moiIsNewestVersion=1 and n.moiDataId in {moiDataIds} return n", session, param);
            if (session != null) {
                if (0 != 0) {
                    try {
                        session.close();
                    } catch (Throwable th2) {
                        th.addSuppressed(th2);
                    }
                } else {
                    session.close();
                }
            }
            return existing;
        } catch (Throwable th3) {
            if (session != null) {
                if (0 != 0) {
                    try {
                        session.close();
                    } catch (Throwable th4) {
                        th.addSuppressed(th4);
                    }
                } else {
                    session.close();
                }
            }
            throw th3;
        }
    }

    @Override // cn.com.bluemoon.metadata.base.service.Neo4jOperatorService
    public List<Map<String, Object>> queryDataByGuid(Driver driver, List<String> guids) {
        Map<String, Object> param = Maps.newHashMap();
        param.put("guids", guids);
        Session session = driver.session();
        Throwable th = null;
        try {
            List<Map<String, Object>> existing = Neo4jDataHelpler.queryNodeForList("match (n:Neo4jNodeBaseEntity) where n.moiIsDelete=0 and n.moiIsNewestVersion=1 and n.guid in {guids} return n", session, param);
            if (session != null) {
                if (0 != 0) {
                    try {
                        session.close();
                    } catch (Throwable th2) {
                        th.addSuppressed(th2);
                    }
                } else {
                    session.close();
                }
            }
            return existing;
        } catch (Throwable th3) {
            if (session != null) {
                if (0 != 0) {
                    try {
                        session.close();
                    } catch (Throwable th4) {
                        th.addSuppressed(th4);
                    }
                } else {
                    session.close();
                }
            }
            throw th3;
        }
    }

    @Override // cn.com.bluemoon.metadata.base.service.Neo4jOperatorService
    public void positiveSync(Driver driver, CreateTypeConfig createTypeConfig, List<Map<String, Object>> positiveIncrement) {
        for (Map<String, Object> positive : positiveIncrement) {
            positive.put("collectType", CollectTypeEnum.UPDATE.getCode());
        }
        Object guidList = (List) positiveIncrement.stream().map(x -> {
            return x.get("guid").toString();
        }).collect(Collectors.toList());
        Map<String, Object> param = new HashMap<>();
        param.put("guids", guidList);
        run(driver, "match (n:Neo4jNodeBaseEntity)-[r]-(l:Neo4jNodeBaseEntity) where n.guid in {guids} delete r", param);
        run(driver, "match (n:Neo4jNodeBaseEntity) where n.guid in {guids} delete n", param);
        batchInsert(driver, createTypeConfig, positiveIncrement);
    }

    @Override // cn.com.bluemoon.metadata.base.service.Neo4jOperatorService
    public void negtiveSync(Driver driver, CreateTypeConfig context, List<Map<String, Object>> negtiveIncrement) {
        for (Map<String, Object> positive : negtiveIncrement) {
            positive.put("collectType", CollectTypeEnum.UPDATE.getCode());
        }
        List<String> guidList = (List) negtiveIncrement.stream().map(x -> {
            return x.get("guid").toString();
        }).collect(Collectors.toList());
        Map<String, Map<String, Object>> negtiveIncrementDict = (Map) negtiveIncrement.stream().collect(Collectors.toMap(x -> {
            return x.get("guid").toString();
        }, y -> {
            return y;
        }, x, y -> {
            return x;
        }));
        Map<String, Object> guids = Maps.newHashMap();
        guids.put("guids", guidList);
        run(driver, "match (n:Neo4jNodeBaseEntity)-[r]-(l:Neo4jNodeBaseEntity) where n.guid in {guids} delete r", guids);
        List<Map<String, Object>> maps = queryDataByGuid(driver, guidList);
        Map<String, Map<String, Object>> mapDict = (Map) maps.stream().collect(Collectors.toMap(x -> {
            return x.get("guid").toString();
        }, y -> {
            return y;
        }, x, y -> {
            return x;
        }));
        NodeCypherGenerator nodeCypherGenerator = new NodeCypherGenerator();
        String createCql = nodeCypherGenerator.generateCreateCql(context);
        for (Map<String, Object> map : maps) {
            map.put("moiIsDelete", 0);
            map.put("moiIsNewestVersion", 0);
        }
        Map<String, Object> param = Maps.newHashMap();
        param.put(Neo4jCypherConstants.DEFAULT_BATCH_KEY, maps);
        run(driver, createCql, param);
        for (Map<String, Object> map2 : maps) {
            String guid = map2.get("guid").toString();
            Map<String, Object> es = mapDict.get(guid);
            Map<String, Object> negtiveMap = negtiveIncrementDict.get(guid);
            negtiveMap.put("moiDataId", map2.get("moiDataId"));
            negtiveMap.put("moiDataIdPath", negtiveMap.get("moiParentDataId") + Neo4jCypherConstants.GUID_DELIMITER + negtiveMap.get("moiDataId"));
            negtiveMap.put("moiDataVersion", Integer.valueOf(Integer.valueOf(es.get("moiDataVersion").toString()).intValue() + 1));
        }
        String updateCql = nodeCypherGenerator.generateUpdateCql(context);
        param.put(Neo4jCypherConstants.DEFAULT_BATCH_KEY, negtiveIncrement);
        run(driver, updateCql, param);
    }
}
