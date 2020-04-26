package com.alibaba.datax.plugin.reader.elasticsearchreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.fastjson.JSON;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.Gson;
import io.searchbox.client.JestResult;
import io.searchbox.core.SearchResult;
import io.searchbox.core.SearchScroll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class ESReader extends Reader {
    public static class Job extends Reader.Job {
        private Configuration conf = null;
        private static final Logger log = LoggerFactory.getLogger(Job.class);


        ESClient esClient = new ESClient();
        private List<ESFieldType> typeList;
        private JSONArray columnList;

        private int trySize;
        private int batchSize;
        private String index;
        private String type;
        private String splitter;

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
        }

        @Override
        public void prepare() {
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：如果 Job 中有需要进行数据同步之前的处理，可以在此处完成，如果没有必要则可以直接去掉。
             */

            esClient.createClient(Key.getEndpoint(conf),
                    Key.getAccessID(conf),
                    Key.getAccessKey(conf),
                    false,
                    300000,
                    false,
                    false);

            String indexName = Key.getIndexName(conf);
            String typeName = Key.getTypeName(conf);
            boolean dynamic = Key.getDynamic(conf);
            String settings = JSONObject.toJSONString(
                    Key.getSettings(conf)
            );
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configurations = new ArrayList<Configuration>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(conf);
            }
            return configurations;
        }

        @Override
        public void post() {
            super.post();
        }

        @Override
        public void destroy() {
        }

    }

    public static class Task extends Reader.Task {
        private Configuration conf = null;
        private static final Logger log = LoggerFactory.getLogger(Job.class);
        private String ScrollId = null;
        public JsonArray hitsData = null;

        ESClient esClient = new ESClient();
        private List<ESFieldType> typeList;
        private  JSONArray columnList;

        private int trySize;
        private int batchSize;
        private String index;
        private String type;
        private String splitter;
        private String search;

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
            index = Key.getIndexName(conf);
            type = Key.getTypeName(conf);
            trySize = Key.getTrySize(conf);
            search = Key.getSearch(conf);
            columnList = JSON.parseArray(Key.getColumn(conf));
        }

        @Override
        public void prepare() {
            esClient.createClient(Key.getEndpoint(conf),
                    Key.getAccessID(conf),
                    Key.getAccessKey(conf),
                    false,
                    300000,
                    false,
                    false);
        }

        @Override
        public void startRead(RecordSender recordSender) {
            try {

                JsonObject res = esClient.seachDocument(search, index, type);
                ScrollId = res.get("_scroll_id").getAsString();
                JsonArray hits = res.get("hits").getAsJsonObject().get("hits").getAsJsonArray();
                Iterator hitsCursor = hits.iterator();
                while (hitsCursor.hasNext()) {
                    Record record = recordSender.createRecord();
                    JsonObject hit = (JsonObject) hitsCursor.next();
                    JsonObject sourceJson = (JsonObject) hit.get("_source");
                    Iterator columnCursor = columnList.iterator();
                    while (columnCursor.hasNext()) {
                        JSONObject columnJson = (JSONObject) columnCursor.next();
                        String columnType = columnJson.get("type").toString();
                        String columnName = columnJson.get("name").toString();
                        Object tempCol = sourceJson.get(columnName);
                        if (tempCol == null) {
                            record.addColumn(new StringColumn(null));
                        } else if (tempCol instanceof Double) {
                            //TODO deal with Double.isNaN()
                            record.addColumn(new DoubleColumn((Double) tempCol));
                        } else if (tempCol instanceof Boolean) {
                            record.addColumn(new BoolColumn((Boolean) tempCol));
                        } else if (tempCol instanceof Date) {
                            record.addColumn(new DateColumn((Date) tempCol));
                        } else if (tempCol instanceof Integer) {
                            record.addColumn(new LongColumn((Integer) tempCol));
                        }else if (tempCol instanceof Long) {
                            record.addColumn(new LongColumn((Long) tempCol));
                        }else {
                            record.addColumn(new StringColumn(tempCol.toString().replace("\"", "")));
                        }

                    }
                    recordSender.sendToWriter(record);
                }
                while (true) {
                    JsonObject resScrll = esClient.searchScroll(ScrollId);
                    JsonArray hitsScrll = resScrll.get("hits").getAsJsonObject().get("hits").getAsJsonArray();
                    Iterator hitsScrllIterator = hitsScrll.iterator();
                    while (hitsScrllIterator.hasNext()) {
                        Record record2 = recordSender.createRecord();
                        JsonObject hitScrll = (JsonObject) hitsScrllIterator.next();
                        JsonObject sourceJsonhitScrll = (JsonObject) hitScrll.get("_source");
                        Iterator columnCursorScrll = columnList.iterator();
                        while (columnCursorScrll.hasNext()) {
                            JSONObject columnJson = (JSONObject) columnCursorScrll.next();
                            String columnType = columnJson.get("type").toString();
                            String columnName = columnJson.get("name").toString();
                            Object tempCol = sourceJsonhitScrll.get(columnName);
                            if (tempCol == null) {
                                record2.addColumn(new StringColumn(null));
                            } else if (tempCol instanceof Double) {
                                //TODO deal with Double.isNaN()
                                record2.addColumn(new DoubleColumn((Double) tempCol));
                            } else if (tempCol instanceof Boolean) {
                                record2.addColumn(new BoolColumn((Boolean) tempCol));
                            } else if (tempCol instanceof Date) {
                                record2.addColumn(new DateColumn((Date) tempCol));
                            } else if (tempCol instanceof Integer) {
                                record2.addColumn(new LongColumn((Integer) tempCol));
                            }else if (tempCol instanceof Long) {
                                record2.addColumn(new LongColumn((Long) tempCol));
                            }else {
                                record2.addColumn(new StringColumn(tempCol.toString()));
                            }

                        }
                        recordSender.sendToWriter(record2);
                    }
                    if (hitsScrll.size() == 0) break;

                }
            } catch (IOException e) {
                throw DataXException.asDataXException(ESReaderErrorCode.ES_CON_ERR, e);
            }

        }


        @Override
        public void post() {
            super.post();
        }

        @Override
        public void destroy() {
        }
    }
}
