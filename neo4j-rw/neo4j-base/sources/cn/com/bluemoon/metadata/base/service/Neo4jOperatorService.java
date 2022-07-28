package cn.com.bluemoon.metadata.base.service;

import cn.com.bluemoon.metadata.base.config.CreateTypeConfig;
import java.util.List;
import java.util.Map;
import org.neo4j.driver.Driver;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/service/Neo4jOperatorService.class */
public interface Neo4jOperatorService {
    void run(Driver driver, String str, Map<String, Object> map);

    void batchInsert(Driver driver, CreateTypeConfig createTypeConfig, List<Map<String, Object>> list);

    void resetNewestToZero(Driver driver, List<String> list);

    List<Map<String, Object>> queryDataByMoiDataIds(Driver driver, List<String> list);

    List<Map<String, Object>> queryDataByGuid(Driver driver, List<String> list);

    void positiveSync(Driver driver, CreateTypeConfig createTypeConfig, List<Map<String, Object>> list);

    void negtiveSync(Driver driver, CreateTypeConfig createTypeConfig, List<Map<String, Object>> list);
}
