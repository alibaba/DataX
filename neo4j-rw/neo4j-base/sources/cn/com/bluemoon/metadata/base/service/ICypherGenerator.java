package cn.com.bluemoon.metadata.base.service;

import cn.com.bluemoon.metadata.base.config.CreateTypeConfig;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/service/ICypherGenerator.class */
public interface ICypherGenerator {
    String generateCreateCql(CreateTypeConfig createTypeConfig);

    String generateUpdateCql(CreateTypeConfig createTypeConfig);
}
