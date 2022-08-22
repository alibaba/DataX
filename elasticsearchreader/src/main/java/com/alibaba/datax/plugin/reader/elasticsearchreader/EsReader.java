package com.alibaba.datax.plugin.reader.elasticsearchreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.exception.ExceptionTracker;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.elasticsearchreader.gson.MapTypeAdapter;
import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import io.searchbox.client.JestResult;
import io.searchbox.core.SearchResult;
import io.searchbox.params.SearchType;
import ognl.Ognl;
import ognl.OgnlException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.*;

/**
 * @author kesc mail:492167585@qq.com
 * @date 2020-04-14 10:32
 */
@SuppressWarnings(value = {"unchecked"})
public class EsReader extends Reader {

    public static class Job extends Reader.Job {
        private static final Logger log = LoggerFactory.getLogger(Job.class);
        private Configuration conf = null;

        @Override
        public void prepare() {
            /*
             * 注意：此方法仅执行一次。
             * 最佳实践：如果 Job 中有需要进行数据同步之前的处理，可以在此处完成，如果没有必要则可以直接去掉。
             */
            ESClient esClient = new ESClient();
            esClient.createClient(Key.getEndpoint(conf),
                    Key.getAccessID(conf),
                    Key.getAccessKey(conf),
                    false,
                    300000,
                    false,
                    false);

            String indexName = Key.getIndexName(conf);
            String typeName = Key.getTypeName(conf);
            log.info("index:[{}], type:[{}]", indexName, typeName);
            try {
                boolean isIndicesExists = esClient.indicesExists(indexName);
                if (!isIndicesExists) {
                    throw new IOException(String.format("index[%s] not exist", indexName));
                }
            } catch (Exception ex) {
                throw DataXException.asDataXException(ESReaderErrorCode.ES_INDEX_NOT_EXISTS, ex.toString());
            }
            esClient.closeJestClient();
        }

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> configurations = new ArrayList<>();
            List<Object> search = conf.getList(Key.SEARCH_KEY, Object.class);
            for (Object query : search) {
                Configuration clone = conf.clone();
                clone.set(Key.SEARCH_KEY, query);
                configurations.add(clone);
            }
            return configurations;
        }

        @Override
        public void post() {
            super.post();
        }

        @Override
        public void destroy() {
            log.info("============elasticsearch reader job destroy=================");
        }
    }

    public static class Task extends Reader.Task {
        private static final Logger log = LoggerFactory.getLogger(Job.class);

        private Configuration conf;
        ESClient esClient = null;
        Gson gson = null;
        private String index;
        private String type;
        private SearchType searchType;
        private Map<String, Object> headers;
        private String query;
        private String scroll;
        private EsTable table;

        @Override
        public void prepare() {
            esClient.createClient(Key.getEndpoint(conf),
                    Key.getAccessID(conf),
                    Key.getAccessKey(conf),
                    Key.isMultiThread(conf),
                    Key.getTimeout(conf),
                    Key.isCompression(conf),
                    Key.isDiscovery(conf));
        }

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
            this.esClient = new ESClient();
            this.gson = new GsonBuilder().registerTypeAdapterFactory(MapTypeAdapter.FACTORY).create();
            this.index = Key.getIndexName(conf);
            this.type = Key.getTypeName(conf);
            this.searchType = Key.getSearchType(conf);
            this.headers = Key.getHeaders(conf);
            this.query = Key.getQuery(conf);
            this.scroll = Key.getScroll(conf);
            this.table = Key.getTable(conf);
            if (table == null || table.getColumn() == null || table.getColumn().isEmpty()) {
                throw DataXException.asDataXException(ESReaderErrorCode.COLUMN_CANT_BE_EMPTY, "请检查job的elasticsearchreader插件下parameter是否配置了table参数");
            }
        }

        @Override
        public void startRead(RecordSender recordSender) {
            PerfTrace.getInstance().addTaskDetails(super.getTaskId(), index);
            //search
            PerfRecord queryPerfRecord = new PerfRecord(super.getTaskGroupId(), super.getTaskId(), PerfRecord.PHASE.SQL_QUERY);
            queryPerfRecord.start();
            SearchResult searchResult;
            try {
                searchResult = esClient.search(query, searchType, index, type, scroll, headers);
            } catch (Exception e) {
                throw DataXException.asDataXException(ESReaderErrorCode.ES_SEARCH_ERROR, e);
            }
            if (!searchResult.isSucceeded()) {
                throw DataXException.asDataXException(ESReaderErrorCode.ES_SEARCH_ERROR, searchResult.getResponseCode() + ":" + searchResult.getErrorMessage());
            }
            queryPerfRecord.end();
            //transport records
            PerfRecord allResultPerfRecord = new PerfRecord(super.getTaskGroupId(), super.getTaskId(), PerfRecord.PHASE.RESULT_NEXT_ALL);
            allResultPerfRecord.start();
            this.transportRecords(recordSender, searchResult);
            allResultPerfRecord.end();
            //do scroll
            JsonElement scrollIdElement = searchResult.getJsonObject().get("_scroll_id");
            if (scrollIdElement == null) {
                return;
            }
            String scrollId = scrollIdElement.getAsString();
            log.info("scroll id:{}", scrollId);
            try {
                boolean hasElement = true;
                while (hasElement) {
                    queryPerfRecord.start();
                    JestResult scroll = esClient.scroll(scrollId, this.scroll);
                    queryPerfRecord.end();
                    if (!scroll.isSucceeded()) {
                        throw DataXException.asDataXException(ESReaderErrorCode.ES_SEARCH_ERROR, String.format("scroll[id=%s] search error,code:%s,msg:%s", scrollId, scroll.getResponseCode(), scroll.getErrorMessage()));
                    }
                    allResultPerfRecord.start();
                    hasElement = this.transportRecords(recordSender, parseSearchResult(scroll));
                    allResultPerfRecord.end();
                }
            } catch (DataXException dxe) {
                throw dxe;
            } catch (Exception e) {
                throw DataXException.asDataXException(ESReaderErrorCode.ES_SEARCH_ERROR, e);
            } finally {
                esClient.clearScroll(scrollId);
            }
        }

        private SearchResult parseSearchResult(JestResult jestResult) {
            if (jestResult == null) {
                return null;
            }
            SearchResult searchResult = new SearchResult(gson);
            searchResult.setSucceeded(jestResult.isSucceeded());
            searchResult.setResponseCode(jestResult.getResponseCode());
            searchResult.setPathToResult(jestResult.getPathToResult());
            searchResult.setJsonString(jestResult.getJsonString());
            searchResult.setJsonObject(jestResult.getJsonObject());
            searchResult.setErrorMessage(jestResult.getErrorMessage());
            return searchResult;
        }

        private void setDefaultValue(List<EsField> column, Map<String, Object> data) {
            for (EsField field : column) {
                if (field.hasChild()) {
                    setDefaultValue(field.getChild(), data);
                } else {
                    data.putIfAbsent(field.getFinalName(table.getNameCase()), null);
                }
            }
        }

        private void getPathSource(List<Map<String, Object>> result, Map<String, Object> source, List<EsField> column, Map<String, Object> parent) {
            if (source.isEmpty()) {
                return;
            }
            for (EsField esField : column) {
                if (!esField.hasChild()) {
                    parent.put(esField.getFinalName(table.getNameCase()), source.getOrDefault(esField.getName(), esField.getValue()));
                }
            }
            for (EsField esField : column) {
                if (!esField.hasChild()) {
                    continue;
                }
                Object value = source.get(esField.getName());
                if (value instanceof Map) {
                    getPathSource(result, (Map<String, Object>) value, esField.getChild(), parent);
                } else if (value instanceof List) {
                    List<Map<String, Object>> valueList = (List<Map<String, Object>>) value;
                    if (valueList.isEmpty()) {
                        continue;
                    }
                    List<Map<String, Object>> joinResults = new ArrayList<>();
                    ArrayList<Map<String, Object>> copyResult = new ArrayList<>(result);
                    result.clear();
                    for (Map<String, Object> joinParent : copyResult) {
                        for (Map<String, Object> item : valueList) {
                            HashMap<String, Object> childData = new LinkedHashMap<>(joinParent);
                            joinResults.add(childData);
                            getPathSource(joinResults, item, esField.getChild(), childData);
                            result.addAll(joinResults);
                            joinResults.clear();
                        }
                    }
                    copyResult.clear();
                }
            }
        }

        private Object getOgnlValue(Object expression, Map<String, Object> root, Object defaultValue) {
            try {
                if (!(expression instanceof String)) {
                    return defaultValue;
                }
                Object value = Ognl.getValue(expression.toString(), root);
                if (value == null) {
                    return defaultValue;
                }
                return value;
            } catch (OgnlException e) {
                return defaultValue;
            }
        }

        private boolean filter(String filter, String deleteFilterKey, Map<String, Object> record) {
            if (StringUtils.isNotBlank(deleteFilterKey)) {
                record.remove(deleteFilterKey);
            }
            if (StringUtils.isBlank(filter)) {
                return true;
            }
            return (Boolean) getOgnlValue(filter, record, Boolean.TRUE);
        }

        private boolean transportRecords(RecordSender recordSender, SearchResult result) {
            if (result == null) {
                return false;
            }
            List<String> sources = result.getSourceAsStringList();
            if (sources == null) {
                sources = Collections.emptyList();
            }
//            log.info("search result: total={},maxScore={},hits={}", result.getTotal(), result.getMaxScore(), sources.size());
            List<Map<String, Object>> recordMaps = new ArrayList<>();
            for (String source : sources) {
                List<EsField> column = table.getColumn();
                if (column == null || column.isEmpty()) {
                    continue;
                }
                Map<String, Object> parent = new LinkedHashMap<>((int) (column.size() * 1.5));
                setDefaultValue(table.getColumn(), parent);
                recordMaps.add(parent);
                getPathSource(recordMaps, gson.fromJson(source, Map.class), column, parent);
                this.transportOneRecord(table, recordSender, recordMaps);
                recordMaps.clear();
            }
            return sources.size() > 0;
        }

        private void transportOneRecord(EsTable table, RecordSender recordSender, List<Map<String, Object>> recordMaps) {
            for (Map<String, Object> o : recordMaps) {
                boolean allow = filter(table.getFilter(), table.getDeleteFilterKey(), o);
                if (allow && o.entrySet().stream().anyMatch(x -> x.getValue() != null)) {
                    Record record = buildRecord(recordSender, o);
                    recordSender.sendToWriter(record);
                }
            }
        }

        private Record buildRecord(RecordSender recordSender, Map<String, Object> source) {
            Record record = recordSender.createRecord();
            boolean hasDirty = false;
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Object> entry : source.entrySet()) {
                try {
                    Object o = source.get(entry.getKey());
                    record.addColumn(getColumn(o));
                } catch (Exception e) {
                    hasDirty = true;
                    sb.append(ExceptionTracker.trace(e));
                }
            }
            if (hasDirty) {
                getTaskPluginCollector().collectDirtyRecord(record, sb.toString());
            }
            return record;
        }

        private Column getColumn(Object value) {
            Column col;
            if (value == null) {
                col = new StringColumn();
            } else if (value instanceof String) {
                col = new StringColumn((String) value);
            } else if (value instanceof Integer) {
                col = new LongColumn(((Integer) value).longValue());
            } else if (value instanceof Long) {
                col = new LongColumn((Long) value);
            } else if (value instanceof Byte) {
                col = new LongColumn(((Byte) value).longValue());
            } else if (value instanceof Short) {
                col = new LongColumn(((Short) value).longValue());
            } else if (value instanceof Double) {
                col = new DoubleColumn(BigDecimal.valueOf((Double) value));
            } else if (value instanceof Float) {
                col = new DoubleColumn(BigDecimal.valueOf(((Float) value).doubleValue()));
            } else if (value instanceof Date) {
                col = new DateColumn((Date) value);
            } else if (value instanceof Boolean) {
                col = new BoolColumn((Boolean) value);
            } else if (value instanceof byte[]) {
                col = new BytesColumn((byte[]) value);
            } else if (value instanceof List) {
                col = new StringColumn(JSON.toJSONString(value));
            } else if (value instanceof Map) {
                col = new StringColumn(JSON.toJSONString(value));
            } else if (value instanceof Array) {
                col = new StringColumn(JSON.toJSONString(value));
            } else {
                throw DataXException.asDataXException(ESReaderErrorCode.UNKNOWN_DATA_TYPE, "type:" + value.getClass().getName());
            }
            return col;
        }

        @Override
        public void post() {
            super.post();
        }

        @Override
        public void destroy() {
            log.info("============elasticsearch reader taskGroup[{}] taskId[{}] destroy=================", super.getTaskGroupId(), super.getTaskId());
            esClient.closeJestClient();
        }
    }
}
