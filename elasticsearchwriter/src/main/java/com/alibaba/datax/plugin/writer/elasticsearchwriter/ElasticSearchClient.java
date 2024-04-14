package com.alibaba.datax.plugin.writer.elasticsearchwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.elasticsearchwriter.jest.ClusterInfo;
import com.alibaba.datax.plugin.writer.elasticsearchwriter.jest.ClusterInfoResult;
import com.alibaba.datax.plugin.writer.elasticsearchwriter.jest.PutMapping7;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.client.config.HttpClientConfig.Builder;
import io.searchbox.core.Bulk;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.aliases.*;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.mapping.PutMapping;

import io.searchbox.indices.settings.GetSettings;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by xiongfeng.bxf on 17/2/8.
 */
public class ElasticSearchClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchClient.class);
    
    private JestClient jestClient;
    private Configuration conf;

    public JestClient getClient() {
        return jestClient;
    }

    public ElasticSearchClient(Configuration conf) {
        this.conf = conf;
        String endpoint = Key.getEndpoint(conf);
        //es是支持集群写入的
        String[] endpoints = endpoint.split(",");
        String user = Key.getUsername(conf);
        String passwd = Key.getPassword(conf);
        boolean multiThread = Key.isMultiThread(conf);
        int readTimeout = Key.getTimeout(conf);
        boolean compression = Key.isCompression(conf);
        boolean discovery = Key.isDiscovery(conf);
        String discoveryFilter = Key.getDiscoveryFilter(conf);
        int totalConnection = this.conf.getInt("maxTotalConnection", 200);
        JestClientFactory factory = new JestClientFactory();
        Builder httpClientConfig = new HttpClientConfig
                .Builder(Arrays.asList(endpoints))
//                .setPreemptiveAuth(new HttpHost(endpoint))
                .multiThreaded(multiThread)
                .connTimeout(readTimeout)
                .readTimeout(readTimeout)
                .maxTotalConnection(totalConnection)
                .requestCompressionEnabled(compression)
                .discoveryEnabled(discovery)
                .discoveryFrequency(5L, TimeUnit.MINUTES)
                .discoveryFilter(discoveryFilter);
        if (!(StringUtils.isBlank(user) || StringUtils.isBlank(passwd))) {
            // 匿名登录
            httpClientConfig.defaultCredentials(user, passwd);
        }
        factory.setHttpClientConfig(httpClientConfig.build());
        this.jestClient = factory.getObject();
    }

    public boolean indicesExists(String indexName) throws Exception {
        boolean isIndicesExists = false;
        JestResult rst = execute(new IndicesExists.Builder(indexName).build());
        if (rst.isSucceeded()) {
            isIndicesExists = true;
        } else {
            LOGGER.warn("IndicesExists got ResponseCode: {} ErrorMessage: {}", rst.getResponseCode(), rst.getErrorMessage());
            switch (rst.getResponseCode()) {
                case 404:
                    isIndicesExists = false;
                    break;
                case 401:
                    // 无权访问
                default:
                    LOGGER.warn(rst.getErrorMessage());
                    break;
            }
        }
        return isIndicesExists;
    }

    public boolean deleteIndex(String indexName) throws Exception {
        LOGGER.info("delete index {}", indexName);
        if (indicesExists(indexName)) {
            JestResult rst = execute(new DeleteIndex.Builder(indexName).build());
            if (!rst.isSucceeded()) {
                LOGGER.warn("DeleteIndex got ResponseCode: {}, ErrorMessage: {}", rst.getResponseCode(), rst.getErrorMessage());
                return false;
            } else {
                LOGGER.info("delete index {} success", indexName);
            }
        } else {
            LOGGER.info("index cannot found, skip delete index {}", indexName);
        }
        return true;
    }

    public boolean isGreaterOrEqualThan7() throws Exception {
        try {
            ClusterInfoResult result = execute(new ClusterInfo.Builder().build());
            LOGGER.info("ClusterInfoResult: {}", result.getJsonString());
            return result.isGreaterOrEqualThan7();
        }catch(Exception e) {
            LOGGER.warn(e.getMessage());
            return false;
        }
    }

    /**
     * 获取索引的settings
     * @param indexName 索引名
     * @return 设置
     */
    public String getIndexSettings(String indexName) {
        GetSettings.Builder builder = new GetSettings.Builder();
        builder.addIndex(indexName);
        GetSettings getSettings = builder.build();
        try {
            LOGGER.info("begin GetSettings for index: {}", indexName);
            JestResult result = this.execute(getSettings);
            return result.getJsonString();
        } catch (Exception e) {
            String message = "GetSettings for index error: " + e.getMessage();
            LOGGER.warn(message, e);
            throw DataXException.asDataXException(ElasticSearchWriterErrorCode.ES_GET_SETTINGS, e.getMessage(), e);
        }
    }

    public boolean createIndexIfNotExists(String indexName, String typeName,
                               Object mappings, String settings,
                               boolean dynamic, boolean isGreaterOrEqualThan7) throws Exception {
        JestResult rst;
        if (!indicesExists(indexName)) {
            LOGGER.info("create index {}", indexName);
            rst = execute(
                    new CreateIndex.Builder(indexName)
                            .settings(settings)
                            .setParameter("master_timeout", Key.getMasterTimeout(this.conf))
                            .build()
            );
            //index_already_exists_exception
            if (!rst.isSucceeded()) {
                LOGGER.warn("CreateIndex got ResponseCode: {}, ErrorMessage: {}", rst.getResponseCode(), rst.getErrorMessage());
                if (getStatus(rst) == 400) {
                    LOGGER.info(String.format("index {} already exists", indexName));
                    return true;
                } else {
                    return false;
                }
            } else {
                LOGGER.info("create {} index success", indexName);
            }
        }

        if (dynamic) {
            LOGGER.info("dynamic is true, ignore mappings");
            return true;
        }
        LOGGER.info("create mappings for {}  {}", indexName, mappings);
        //如果大于7.x，mapping的PUT请求URI中不能带type，并且mapping设置中不能带有嵌套结构
        if (isGreaterOrEqualThan7) {
            rst = execute(new PutMapping7.Builder(indexName, mappings).
                setParameter("master_timeout", Key.getMasterTimeout(this.conf)).build());
        } else {
            rst = execute(new PutMapping.Builder(indexName, typeName, mappings)
                .setParameter("master_timeout", Key.getMasterTimeout(this.conf)).build());
        }
        if (!rst.isSucceeded()) {
            LOGGER.error("PutMapping got ResponseCode: {}, ErrorMessage: {}", rst.getResponseCode(), rst.getErrorMessage());
            return false;
        } else {
            LOGGER.info("index {} put mappings success", indexName);
        }
        return true;
    }

    public <T extends JestResult> T execute(Action<T> clientRequest) throws IOException {
        T rst = jestClient.execute(clientRequest);
        if (!rst.isSucceeded()) {
            LOGGER.warn(rst.getJsonString());
        }
        return rst;
    }

    public Integer getStatus(JestResult rst) {
        JsonObject jsonObject = rst.getJsonObject();
        if (jsonObject.has("status")) {
            return jsonObject.get("status").getAsInt();
        }
        return 600;
    }

    public boolean isBulkResult(JestResult rst) {
        JsonObject jsonObject = rst.getJsonObject();
        return jsonObject.has("items");
    }


    public boolean alias(String indexname, String aliasname, boolean needClean) throws IOException {
        GetAliases getAliases = new GetAliases.Builder().addIndex(aliasname).build();
        AliasMapping addAliasMapping = new AddAliasMapping.Builder(indexname, aliasname).build();
        JestResult rst = null;
        List<AliasMapping> list = new ArrayList<AliasMapping>();
        if (needClean) {
            rst = execute(getAliases);
            if (rst.isSucceeded()) {
                JsonParser jp = new JsonParser();
                JsonObject jo = (JsonObject) jp.parse(rst.getJsonString());
                for (Map.Entry<String, JsonElement> entry : jo.entrySet()) {
                    String tindex = entry.getKey();
                    if (indexname.equals(tindex)) {
                        continue;
                    }
                    AliasMapping m = new RemoveAliasMapping.Builder(tindex, aliasname).build();
                    String s = new Gson().toJson(m.getData());
                    LOGGER.info(s);
                    list.add(m);
                }
            }
        }

        ModifyAliases modifyAliases = new ModifyAliases.Builder(addAliasMapping).addAlias(list).setParameter("master_timeout", Key.getMasterTimeout(this.conf)).build();
        rst = execute(modifyAliases);
        if (!rst.isSucceeded()) {
            LOGGER.error(rst.getErrorMessage());
            throw new IOException(rst.getErrorMessage());
        }
        return true;
    }
    
    /**
     * 获取index的mapping
     */
    public String getIndexMapping(String indexName) {
        GetMapping.Builder builder = new GetMapping.Builder();
        builder.addIndex(indexName);
        GetMapping getMapping = builder.build();
        try {
            LOGGER.info("begin GetMapping for index: {}", indexName);
            JestResult result = this.execute(getMapping);
            return result.getJsonString();
        } catch (Exception e) {
            String message = "GetMapping for index error: " + e.getMessage();
            LOGGER.warn(message, e);
            throw DataXException.asDataXException(ElasticSearchWriterErrorCode.ES_MAPPINGS, e.getMessage(), e);
        }
    }
    
    public String getMappingForIndexType(String indexName, String typeName) {
        String indexMapping = this.getIndexMapping(indexName);
        JSONObject indexMappingInJson = JSON.parseObject(indexMapping);
        List<String> paths = Arrays.asList(indexName, "mappings");
        JSONObject properties = JsonPathUtil.getJsonObject(paths, indexMappingInJson);
        JSONObject propertiesParent = properties;
        if (StringUtils.isNotBlank(typeName) && properties.containsKey(typeName)) {
            propertiesParent = (JSONObject) properties.get(typeName);
        }
        JSONObject mapping = (JSONObject) propertiesParent.get("properties");
        return JSON.toJSONString(mapping);
    }

    public JestResult bulkInsert(Bulk.Builder bulk) throws Exception {
        // es_rejected_execution_exception
        // illegal_argument_exception
        // cluster_block_exception
        JestResult rst = null;
        rst = execute(bulk.build());
        if (!rst.isSucceeded()) {
            LOGGER.warn(rst.getErrorMessage());
        }
        return rst;
    }

    /**
     * 关闭JestClient客户端
     *
     */
    public void closeJestClient() {
        if (jestClient != null) {
            try {
                // jestClient.shutdownClient();
                jestClient.close();
            } catch (IOException e) {
                LOGGER.warn("ignore error: ", e.getMessage());
            }

        }
    }
}
