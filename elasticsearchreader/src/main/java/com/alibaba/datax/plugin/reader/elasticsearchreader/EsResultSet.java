package com.alibaba.datax.plugin.reader.elasticsearchreader;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class EsResultSet {

    private static final Logger LOG = LoggerFactory
            .getLogger(EsResultSet.class);

    private RestHighLevelClient client = null;
    private JSONArray list = null;
    private String scrollId = null;
    private Scroll scroll  = null;

    public EsResultSet() {
    }


    public EsResultSet(RestHighLevelClient client, String scrollId, JSONArray list,Scroll scroll) {
        this.client = client;
        this.scrollId = scrollId;
        this.list = list;
        this.scroll = scroll;

    }


    public boolean hasNext() {
        if (list.size() > 0) {
            return true;
        }

        searchScroll();
        if (list.size() > 0) {
            return true;
        }
        return false;


    }


    private void searchScroll() {
        SearchHit[] searchHits;
        if (StringUtils.isBlank(this.scrollId)) {
            return;
        }
        SearchScrollRequest scrollRequest = new SearchScrollRequest(this.scrollId);
        scrollRequest.scroll(this.scroll);
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.searchScroll(scrollRequest);

            searchHits = searchResponse.getHits().getHits();

            List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
            for (SearchHit searchHit : searchHits) {
                String source = searchHit.getSourceAsString();
                JSONObject parseObject = JSONObject.parseObject(source);
                list.add(parseObject);
            }

            this.scrollId = searchResponse.getScrollId();

        } catch (IOException e) {
            e.printStackTrace();
        }


    }


    public JSONObject next() {

        return (JSONObject) this.list.remove(0);

    }

    /**
     * 清理游标
     *
     * @return
     */
    public void close() {
        if (StringUtils.isBlank(this.scrollId)) {
            return;
        }
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(this.scrollId);
        try {
            ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
