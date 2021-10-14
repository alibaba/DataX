package com.alibaba.datax.plugin.reader.elasticsearchreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.SearchScroll;
import io.searchbox.params.Parameters;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

public class ESReader extends Reader {

    public static class Job extends Reader.Job {
        private static Logger logger = LoggerFactory.getLogger(Job.class);
        private Configuration configuration = null;
        private ESClient esClient;
        private String index = "";
        private String type = "";
        private int shardsNumber;

        @Override
        public void init() {
            logger.info("=================elasticsearch reader job init======================");
            configuration = this.getPluginJobConf();
            this.index = configuration.getString(ConfigConstants.INDEX);
            this.type = configuration.getString(ConfigConstants.TYPE);
            this.shardsNumber = configuration.getInt(ConfigConstants.SHARDNUM);
            esClient = new ESClient();
            esClient.createClient(Key.getEndpoint(configuration),
                    Key.getAccessID(configuration),
                    Key.getAccessKey(configuration),
                    true,
                    300000,
                    false,
                    false);
//            GetSettingsResponse response = client.admin().indices().prepareGetSettings(this.index).get();
//            for (ObjectObjectCursor<String, Settings> cursor : response.getIndexToSettings()) {
//                String index = cursor.key;
//                Settings settings = cursor.value;
//                Integer shards = settings.getAsInt("index.number_of_shards", null);
//                if(StringUtils.equals(index,this.index)){
//                    this.shardsNumber = shards;
//                }
//            }

            esClient.closeJestClient();
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
//            List<Configuration> readerSplitConfiguration = new ArrayList<Configuration>();
//            for (int i = 0; i < adviceNumber; i++) {
//                readerSplitConfiguration.add(this.configuration);
//            }
//            return readerSplitConfiguration;

            //设置并发shards
            int taskNum = adviceNumber;
            if (this.shardsNumber <= taskNum) {
                taskNum = this.shardsNumber;
            }
            int shardsCode[] = new int[this.shardsNumber];
            for (int i = 0; i < this.shardsNumber; i++) {
                shardsCode[i] = i;
            }
            int shardNumPerTask = (int) Math.round(((double) this.shardsNumber) / taskNum);
            List<Configuration> configs = new ArrayList<Configuration>();
            for (int i = 0; i < taskNum; i++) {
                int start = i * shardNumPerTask;
                int end = start + shardNumPerTask;
                if (end > shardsCode.length || (i == taskNum - 1 && end < shardsCode.length)) {
                    end = shardsCode.length;
                }
                int[] shardsArr = ArrayUtils.subarray(shardsCode, start, end);
                Configuration shardConfig = this.configuration.clone();
                shardConfig.set("shards", StringUtils.join(shardsArr, ",".charAt(0)));
                configs.add(shardConfig);
            }
            return configs;
        }

        @Override
        public void destroy() {
            logger.info("============elasticsearch reader job destroy=================");
        }
    }

    public static class Task extends Reader.Task {
        private static Logger logger = LoggerFactory.getLogger(Task.class);
        private ESClient esClient;
        private Configuration configuration = null;
        private String index = "";
        private String type = "";
        private List<String> column = null;
        private String scrollId;
        private TimeValue keepAlive = TimeValue.timeValueMinutes(3);
        private int pageSize = 100;
        private String shards;
        private String queryJson = "";
        private Configuration conf;


        @Override
        public void init() {
            logger.info("=================elasticsearch reader task init======================");
            this.conf = super.getPluginJobConf();
            configuration = super.getPluginJobConf();
            this.index = configuration.getString(ConfigConstants.INDEX);
            this.type = configuration.getString(ConfigConstants.TYPE);
            this.column = configuration.getList(ConfigConstants.COLUMN, String.class);
            this.pageSize = configuration.getInt(ConfigConstants.PAGE_SIZE, this.pageSize);
            this.shards = configuration.getString("shards");
            esClient = new ESClient();
            esClient.createClient(Key.getEndpoint(conf),
                    Key.getAccessID(conf),
                    Key.getAccessKey(conf),
                    false,
                    300000,
                    false,
                    false);
            this.queryJson = configuration.getString(ConfigConstants.WHERE);
            //todo 获取shards

        }

        @Override
        public void startRead(RecordSender recordSender) {
            logger.info("=============elasticsearch reader task start read on shards:" + this.shards + "==================");
            String SCROLL_ALIVE_TIME = "1m";
            JestResult result = null;
            try {
                while (true) {
                    //首次查询
                    // todo 滚动时间超时,则重新查询
                    if (StringUtils.isEmpty(scrollId)) {
                        //循环构造查询条件
                        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                        if (null != queryJson && "" != queryJson) {
                            searchSourceBuilder.query(queryJson);
                        }
                        searchSourceBuilder.size(pageSize);
                        //构造查询条件,设置索引及类型
                        Search search = new Search.Builder(searchSourceBuilder.toString())
                                .addIndex(index)
                                .addType(type)
                                .setParameter(Parameters.SCROLL, SCROLL_ALIVE_TIME)
//                                .setParameter("_shards",shards)//todo shard有问题
                                .build();
                        //第一次检索,拍下快照
                        result = esClient.exe(search);

                        if (result != null && !result.isSucceeded()) {
                            throw DataXException.asDataXException(ESReaderErrorCode.QUERY_FAILED, "ES查询失败，请检查查询条件：" + queryJson);
                        }
                    } else {
                        SearchScroll scroll = new SearchScroll.Builder(scrollId, SCROLL_ALIVE_TIME).build();
                        result = esClient.exeScroll(scroll);
                        if (result.getJsonObject().getAsJsonObject("hits").getAsJsonArray("hits").size() == 0) {
                            logger.info("=================elasticsearch reader task end read======================");
                            esClient.clearScrollIds(this.scrollId);
                            recordSender.flush();
                            recordSender.terminate();
                            return;
                        }

                    }

                    this.scrollId = result.getJsonObject().get("_scroll_id").getAsString();
                    JsonArray jsonElements = result.getJsonObject().getAsJsonObject("hits").getAsJsonArray("hits");

                    for (JsonElement jsonElement : jsonElements) {
                        //获取所有内容
                        Map<String, Object> data = (Map) JSON.parse(jsonElement.getAsJsonObject().get("_source").getAsJsonObject().toString());
                        Record record = recordSender.createRecord();
                        String _id = jsonElement.getAsJsonObject().get("_id").getAsString();
                        Column _idCol = new StringColumn(_id);//es的doc id
                        if (this.column.size() == 1 && StringUtils.equals("*", this.column.get(0))) {
                            record.addColumn(_idCol);
                            Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
                            while (iterator.hasNext()) {
                                Map.Entry<String, Object> entry = iterator.next();
                                Column col = getColumn(_id, entry.getKey(), entry.getValue());
                                record.addColumn(col);
                            }
                        } else {
                            for (int j = 0; j < this.column.size(); j++) {
                                String key = this.column.get(j);
                                if (StringUtils.equals("_id", key)) {
                                    record.addColumn(_idCol);
                                } else if (key.startsWith("'") && key.endsWith("'")) {
                                    //常量'2020-11-20'
                                    Column col = getColumn(_id, key, key.substring(1, key.length() - 1));
                                    record.addColumn(col);
                                } else {
                                    Column col = getColumn(_id, key, data.get(key));
                                    record.addColumn(col);
                                }

                            }
                        }
                        recordSender.sendToWriter(record);
                    }
                }
            } catch (IOException e) {
                logger.error("获取列失败:" + e);
            }
        }

        @Override
        public void destroy() {
            logger.info("======elasticsearch reader task destroy==============");
            this.esClient.closeJestClient();
        }

        private Column getColumn(String _id, String key, Object value) {
            if (value == null) {
                return new StringColumn(null);
            }
            Column col = null;
            if (value instanceof Long) {
                col = new LongColumn((Long) value);
            } else if (value instanceof Integer) {
                col = new LongColumn(((Integer) value).longValue());
            } else if (value instanceof Byte) {
                col = new LongColumn(((Byte) value).longValue());
            } else if (value instanceof Short) {
                col = new LongColumn(((Short) value).longValue());
            } else if (value instanceof String) {
                col = new StringColumn((String) value);
            } else if (value instanceof Double) {
                col = new DoubleColumn((Double) value);
            } else if (value instanceof Float) {
                col = new DoubleColumn(((Float) value).doubleValue());
            } else if (value instanceof Date) {
                col = new DateColumn((Date) value);
            } else if (value instanceof Boolean) {
                col = new BoolColumn((Boolean) value);
            } else if (value instanceof byte[]) {
                col = new BytesColumn((byte[]) value);
            } else if (value instanceof BigDecimal) {
                col = new DoubleColumn((BigDecimal) value);
            } else {
                throw DataXException.asDataXException(ESReaderErrorCode.UNKNOWN_DATA_TYPE, "发生在_id:" + _id + ",key:" + key);
            }
            return col;
        }
    }

}
