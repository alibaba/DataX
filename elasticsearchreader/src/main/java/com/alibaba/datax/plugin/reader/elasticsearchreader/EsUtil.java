package com.alibaba.datax.plugin.reader.elasticsearchreader;


import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Utilities for ElasticSearch
 * <p>
 * Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class EsUtil {
    private String scrollId = null;
    private  Scroll scroll;


    public static RestHighLevelClient getClient(List<Object> list, String username, String password, Configuration config) {
        List<HttpHost> httpHostList = new ArrayList<>();
        for (Object add : list) {
            String address = (String)add;
            String[] pair = address.split(":");
            httpHostList.add(new HttpHost(pair[0], Integer.parseInt(pair[1]), "http"));
        }

        RestClientBuilder builder = RestClient.builder(httpHostList.toArray(new HttpHost[0]));

        Integer timeout = config.getInt(Key.timeout);
        if (timeout != null) {
            builder.setMaxRetryTimeoutMillis(timeout * 1000);
        }
        String pathPrefix = config.getString(Key.pathPrefix);
        if (StringUtils.isNotEmpty(pathPrefix)) {
            builder.setPathPrefix(pathPrefix);
        }
        if (StringUtils.isNotBlank(username)) {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            builder.setHttpClientConfigCallback(httpClientBuilder -> {
                httpClientBuilder.disableAuthCaching();
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            });
        }

        return new RestHighLevelClient(builder);
    }


    public Iterator<Map<String, Object>> searchScroll(SearchRequest searchRequest, RestHighLevelClient client) throws IOException {
        SearchHit[] searchHits;
        if (scrollId == null) {
            SearchResponse searchResponse = client.search(searchRequest);
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
        } else {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            SearchResponse searchResponse = client.searchScroll(scrollRequest);
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
        }

        List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
        for (SearchHit searchHit : searchHits) {
            Map<String, Object> source = searchHit.getSourceAsMap();
            resultList.add(source);
        }

        Iterator<Map<String, Object>> iterator = resultList.iterator();
        return iterator;
    }




    public  EsResultSet iterator(RestHighLevelClient client,SearchRequest  searchRequest,Scroll scroll) {
        this.scroll = scroll;
        SearchResponse searchResponse = null;
        EsResultSet esResultSet = null;
        try {
            searchResponse = client.search(searchRequest);
            String scrollId = searchResponse.getScrollId();

            SearchHit[] searchHits = searchResponse.getHits().getHits();


            JSONArray list = new JSONArray();
            for (SearchHit searchHit : searchHits) {
                String source = searchHit.getSourceAsString();
                JSONObject parseObject = JSONObject.parseObject(source);
                list.add(parseObject);
            }

            esResultSet= new EsResultSet(client, scrollId, list, this.scroll );
        } catch (IOException e) {
            e.printStackTrace();
        }

        return esResultSet;

    }
}