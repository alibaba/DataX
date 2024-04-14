package com.alibaba.datax.plugin.writer.elasticsearchwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.DataXCaseEnvUtil;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.writer.elasticsearchwriter.Key.ActionType;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.TypeReference;
import com.alibaba.fastjson2.JSONWriter;
import com.google.common.base.Joiner;
import io.searchbox.client.JestResult;
import io.searchbox.core.*;
import io.searchbox.params.Parameters;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;

public class ElasticSearchWriter extends Writer {
    private final static String WRITE_COLUMNS = "write_columns";

    public static class Job extends Writer.Job {
        private static final Logger LOGGER = LoggerFactory.getLogger(Job.class);

        private Configuration conf = null;
        int retryTimes = 3;
        long sleepTimeInMilliSecond = 10000L;

        private String settingsCache;

        private void setSettings(String settings) {
            this.settingsCache = JsonUtil.mergeJsonStr(settings, this.settingsCache);
        }

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
            //LOGGER.info("conf:{}", conf);
            this.retryTimes = this.conf.getInt("retryTimes", 3);
            this.sleepTimeInMilliSecond = this.conf.getLong("sleepTimeInMilliSecond", 10000L);
        }

        public List<String> getIncludeSettings() {
              return this.conf.getList("includeSettingKeys", Arrays.asList("number_of_shards", "number_of_replicas"), String.class);
        }

        /**
         * 从es中获取的原始settings转为需要的settings
         * @param originSettings 原始settings
         * @return settings
         */
        private String convertSettings(String originSettings) {
            if(StringUtils.isBlank(originSettings)) {
                return null;
            }
            JSONObject jsonObject = JSON.parseObject(originSettings);
            for(String key : jsonObject.keySet()) {
                JSONObject settingsObj = jsonObject.getJSONObject(key);
                if(settingsObj != null) {
                    JSONObject indexObj = settingsObj.getJSONObject("settings");
                    JSONObject settings = indexObj.getJSONObject("index");
                    JSONObject filterSettings = new JSONObject();
                    if(settings != null) {
                        List<String> includeSettings = getIncludeSettings();
                        if(includeSettings != null && includeSettings.size() > 0) {
                            for(String includeSetting : includeSettings) {
                                Object fieldValue = settings.get(includeSetting);
                                if(fieldValue != null) {
                                    filterSettings.put(includeSetting, fieldValue);
                                }
                            }
                            return filterSettings.toJSONString();
                        }
                    }
                }
            }
            return null;
        }

        @Override
        public void prepare() {
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：如果 Job 中有需要进行数据同步之前的处理，可以在此处完成，如果没有必要则可以直接去掉。
             * 对于7.x之后的es版本，取消了index设置type的逻辑，因此在prepare阶段，加入了判断是否为7.x及以上版本
             * 如果是7.x及以上版本，需要对于index的type做不同的处理
             * 详见 : https://www.elastic.co/guide/en/elasticsearch/reference/6.8/removal-of-types.html
             */
            final ElasticSearchClient esClient = new ElasticSearchClient(this.conf);
            final String indexName = Key.getIndexName(conf);
            ActionType actionType = Key.getActionType(conf);
            final String typeName = Key.getTypeName(conf);
            final boolean dynamic = Key.getDynamic(conf);
            final String dstDynamic = Key.getDstDynamic(conf);
            final String newSettings = JSONObject.toJSONString(Key.getSettings(conf));
            LOGGER.info("conf settings:{}, settingsCache:{}", newSettings, this.settingsCache);
            final Integer esVersion = Key.getESVersion(conf);
            boolean hasId = this.hasID();
            this.conf.set("hasId", hasId);
            if (ActionType.UPDATE.equals(actionType) && !hasId && !hasPrimaryKeyInfo()) {
                throw DataXException.asDataXException(ElasticSearchWriterErrorCode.UPDATE_WITH_ID, "Update mode must specify column type with id or primaryKeyInfo config");
            }
            
            try {
                RetryUtil.executeWithRetry(() -> {
                    boolean isGreaterOrEqualThan7 = esClient.isGreaterOrEqualThan7();
                    if (esVersion != null && esVersion >= 7) {
                        isGreaterOrEqualThan7 = true;
                    }
                    String mappings = genMappings(dstDynamic, typeName, isGreaterOrEqualThan7);
                    conf.set("isGreaterOrEqualThan7", isGreaterOrEqualThan7);

                    
                    LOGGER.info(String.format("index:[%s], type:[%s], mappings:[%s]", indexName, typeName, mappings));
                    boolean isIndicesExists = esClient.indicesExists(indexName);
                    if (isIndicesExists) {
                        try {
                            // 将原有的mapping打印出来，便于排查问题
                            String oldMappings = esClient.getMappingForIndexType(indexName, typeName);
                            LOGGER.info("the mappings for old index is: {}", oldMappings);
                        } catch (Exception e) {
                            LOGGER.warn("warn message: {}", e.getMessage());
                        }
                    }
                    
                    if (Key.isTruncate(conf) && isIndicesExists) {
                        // 备份老的索引中的settings到缓存
                        try {
                            String oldOriginSettings = esClient.getIndexSettings(indexName);
                            if (StringUtils.isNotBlank(oldOriginSettings)) {
                                String includeSettings = convertSettings(oldOriginSettings);
                                LOGGER.info("merge1 settings:{}, settingsCache:{}, includeSettings:{}",
                                    oldOriginSettings,
                                    this.settingsCache, includeSettings);
                                this.setSettings(includeSettings);
                            }
                        } catch (Exception e) {
                            LOGGER.warn("get old settings fail, indexName:{}", indexName);
                        }
                        esClient.deleteIndex(indexName);
                    }

                    // 更新缓存中的settings
                    this.setSettings(newSettings);
                    LOGGER.info("merge2 settings:{}, settingsCache:{}", newSettings, this.settingsCache);
                    // 强制创建,内部自动忽略已存在的情况
                    if (!esClient.createIndexIfNotExists(indexName, typeName, mappings, this.settingsCache, dynamic,
                            isGreaterOrEqualThan7)) {
                        throw DataXException.asDataXException(ElasticSearchWriterErrorCode.ES_MAPPINGS, "");
                    }

                    return true;
                }, DataXCaseEnvUtil.getRetryTimes(this.retryTimes), DataXCaseEnvUtil.getRetryInterval(this.sleepTimeInMilliSecond), DataXCaseEnvUtil.getRetryExponential(false));
            } catch (Exception ex) {
                throw DataXException.asDataXException(ElasticSearchWriterErrorCode.ES_MAPPINGS, ex.getMessage(), ex);
            } finally {
                try {
                    esClient.closeJestClient();
                } catch (Exception e) {
                    LOGGER.warn("ignore close jest client error: {}", e.getMessage());
                }
            }
        }

        private boolean hasID() {
            List column = conf.getList("column");
            if (column != null) {
                for (Object col : column) {
                    JSONObject jo = JSONObject.parseObject(col.toString());
                    String colTypeStr = jo.getString("type");
                    ElasticSearchFieldType colType = ElasticSearchFieldType.getESFieldType(colTypeStr);
                    if (ElasticSearchFieldType.ID.equals(colType)) {
                        return true;
                    }
                }
            }
            return false;
        }
        
        private boolean hasPrimaryKeyInfo() {
            PrimaryKeyInfo primaryKeyInfo = Key.getPrimaryKeyInfo(this.conf);
            if (null != primaryKeyInfo && null != primaryKeyInfo.getColumn() && !primaryKeyInfo.getColumn().isEmpty()) {
                return true;
            } else {
                return false;
            }
        }
        
        
        private String genMappings(String dstDynamic, String typeName, boolean isGreaterOrEqualThan7) {
            String mappings;
            Map<String, Object> propMap = new HashMap<String, Object>();
            List<ElasticSearchColumn> columnList = new ArrayList<ElasticSearchColumn>();
            ElasticSearchColumn combineItem = null;

            List column = conf.getList("column");
            if (column != null) {
                for (Object col : column) {
                    JSONObject jo = JSONObject.parseObject(col.toString());
                    String colName = jo.getString("name");
                    String colTypeStr = jo.getString("type");
                    if (colTypeStr == null) {
                        throw DataXException.asDataXException(ElasticSearchWriterErrorCode.BAD_CONFIG_VALUE, col.toString() + " column must have type");
                    }
                    ElasticSearchFieldType colType = ElasticSearchFieldType.getESFieldType(colTypeStr);
                    if (colType == null) {
                        throw DataXException.asDataXException(ElasticSearchWriterErrorCode.BAD_CONFIG_VALUE, col.toString() + " unsupported type");
                    }

                    ElasticSearchColumn columnItem = new ElasticSearchColumn();

                    if (Key.PRIMARY_KEY_COLUMN_NAME.equals(colName)) {
                        // 兼容已有版本
                        colType = ElasticSearchFieldType.ID;
                        colTypeStr = "id";
                    }

                    columnItem.setName(colName);
                    columnItem.setType(colTypeStr);

                    JSONArray combineFields = jo.getJSONArray("combineFields");
                    if (combineFields != null && !combineFields.isEmpty() && ElasticSearchFieldType.ID.equals(ElasticSearchFieldType.getESFieldType(colTypeStr))) {
                        List<String> fields = new ArrayList<String>();
                        for (Object item : combineFields) {
                            fields.add((String) item);
                        }
                        columnItem.setCombineFields(fields);
                        combineItem = columnItem;
                    }

                    String combineFieldsValueSeparator = jo.getString("combineFieldsValueSeparator");
                    if (StringUtils.isNotBlank(combineFieldsValueSeparator)) {
                        columnItem.setCombineFieldsValueSeparator(combineFieldsValueSeparator);
                    }

                    // 如果是id，version，routing，不需要创建mapping
                    if (colType == ElasticSearchFieldType.ID || colType == ElasticSearchFieldType.VERSION || colType == ElasticSearchFieldType.ROUTING) {
                        columnList.add(columnItem);
                        continue;
                    }

                    // 如果是组合id中的字段，不需要创建mapping
                    // 所以组合id的定义必须要在columns最前面
                    if (combineItem != null && combineItem.getCombineFields().contains(colName)) {
                        columnList.add(columnItem);
                        continue;
                    }
                    columnItem.setDstArray(false);
                    Boolean array = jo.getBoolean("array");
                    if (array != null) {
                        columnItem.setArray(array);
                        Boolean dstArray = jo.getBoolean("dstArray");
                        if(dstArray!=null) {
                            columnItem.setDstArray(dstArray);
                        }
                    } else {
                        columnItem.setArray(false);
                    }
                    Boolean jsonArray = jo.getBoolean("json_array");
                    if (jsonArray != null) {
                        columnItem.setJsonArray(jsonArray);
                    } else {
                        columnItem.setJsonArray(false);
                    }
                    Map<String, Object> field = new HashMap<String, Object>();
                    field.put("type", colTypeStr);
                    //https://www.elastic.co/guide/en/elasticsearch/reference/5.2/breaking_50_mapping_changes.html#_literal_index_literal_property
                    // https://www.elastic.co/guide/en/elasticsearch/guide/2.x/_deep_dive_on_doc_values.html#_disabling_doc_values
                    field.put("doc_values", jo.getBoolean("doc_values"));
                    field.put("ignore_above", jo.getInteger("ignore_above"));
                    field.put("index", jo.getBoolean("index"));
                    switch (colType) {
                        case STRING:
                            // 兼容string类型,ES5之前版本
                            break;
                        case KEYWORD:
                            // https://www.elastic.co/guide/en/elasticsearch/reference/current/tune-for-search-speed.html#_warm_up_global_ordinals
                            field.put("eager_global_ordinals", jo.getBoolean("eager_global_ordinals"));
                            break;
                        case TEXT:
                            field.put("analyzer", jo.getString("analyzer"));
                            // 优化disk使用,也同步会提高index性能
                            // https://www.elastic.co/guide/en/elasticsearch/reference/current/tune-for-disk-usage.html
                            field.put("norms", jo.getBoolean("norms"));
                            field.put("index_options", jo.getBoolean("index_options"));
                            if(jo.getString("fields") != null) {
                                field.put("fields", jo.getJSONObject("fields"));
                            }
                            break;
                        case DATE:
                            if (Boolean.TRUE.equals(jo.getBoolean("origin"))) {
                                if (jo.getString("format") != null) {
                                    field.put("format", jo.getString("format"));
                                }
                                // es原生format覆盖原先来的format
                                if (jo.getString("dstFormat") != null) {
                                    field.put("format", jo.getString("dstFormat"));
                                }
                                if(jo.getBoolean("origin") != null) {
                                    columnItem.setOrigin(jo.getBoolean("origin"));
                                }
                            } else {
                                columnItem.setTimeZone(jo.getString("timezone"));
                                columnItem.setFormat(jo.getString("format"));
                            }
                            break;
                        case GEO_SHAPE:
                            field.put("tree", jo.getString("tree"));
                            field.put("precision", jo.getString("precision"));
                            break;
                        case OBJECT:
                        case NESTED:
                            if (jo.getString("dynamic") != null) {
                                field.put("dynamic", jo.getString("dynamic"));
                            }
                            break;
                        default:
                            break;
                    }
                    if (jo.containsKey("other_params")) {
                        field.putAll(jo.getJSONObject("other_params"));
                    }
                    propMap.put(colName, field);
                    columnList.add(columnItem);
                }
            }

            long version = System.currentTimeMillis();
            LOGGER.info("unified version: {}", version);
            conf.set("version", version);
            conf.set(WRITE_COLUMNS, JSON.toJSONString(columnList));

            LOGGER.info(JSON.toJSONString(columnList));

            Map<String, Object> rootMappings = new HashMap<String, Object>();
            Map<String, Object> typeMappings = new HashMap<String, Object>();
            typeMappings.put("properties", propMap);
            rootMappings.put(typeName, typeMappings);

            // 7.x以后版本取消了index中关于type的指定，所以mapping的格式只能支持
            // {
            //      "properties" : {
            //          "abc" : {
            //              "type" : "text"
            //              }
            //           }
            // }
            // properties 外不能再嵌套typeName

            if(StringUtils.isNotBlank(dstDynamic)) {
                typeMappings.put("dynamic", dstDynamic);
            }
            if (isGreaterOrEqualThan7) {
                mappings = JSON.toJSONString(typeMappings);
            } else {
                mappings = JSON.toJSONString(rootMappings);
            }
            if (StringUtils.isBlank(mappings)) {
                throw DataXException.asDataXException(ElasticSearchWriterErrorCode.BAD_CONFIG_VALUE, "must have mappings");
            }

            return mappings;
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configurations = new ArrayList<Configuration>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(this.conf.clone());
            }
            return configurations;
        }

        @Override
        public void post() {
            ElasticSearchClient esClient = new ElasticSearchClient(this.conf);
            String alias = Key.getAlias(conf);
            if (!"".equals(alias)) {
                LOGGER.info(String.format("alias [%s] to [%s]", alias, Key.getIndexName(conf)));
                try {
                    esClient.alias(Key.getIndexName(conf), alias, Key.isNeedCleanAlias(conf));
                } catch (IOException e) {
                    throw DataXException.asDataXException(ElasticSearchWriterErrorCode.ES_ALIAS_MODIFY, e);
                }
            }
        }

        @Override
        public void destroy() {

        }
    }

    public static class Task extends Writer.Task {

        private static final Logger LOGGER = LoggerFactory.getLogger(Job.class);

        private Configuration conf;


        ElasticSearchClient esClient = null;
        private List<ElasticSearchFieldType> typeList;
        private List<ElasticSearchColumn> columnList;
        private List<Map<String, Object>> deleteByConditions;

        private int trySize;
        private long tryInterval;
        private int batchSize;
        private String index;
        private String type;
        private String splitter;
        private ActionType actionType;
        private ElasticSearchColumn combinedIdColumn;
        private Map<String, Integer> colNameToIndexMap;
        private Map<String, Object> urlParams;
        private boolean columnSizeChecked = false;
        private boolean enableRedundantColumn = false;
        private boolean enableWriteNull = true;
        int retryTimes = 3;
        long sleepTimeInMilliSecond = 10000L;
        boolean isGreaterOrEqualThan7 = false;
        private String fieldDelimiter;
        private boolean hasId;
        private PrimaryKeyInfo primaryKeyInfo;
        private boolean hasPrimaryKeyInfo = false;
        private List<PartitionColumn> esPartitionColumn;
        private boolean hasEsPartitionColumn = false;

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
            this.index = Key.getIndexName(conf);
            this.type = Key.getTypeName(conf);
            this.trySize = Key.getTrySize(conf);
            this.tryInterval = Key.getTryInterval(conf);
            this.batchSize = Key.getBatchSize(conf);
            this.splitter = Key.getSplitter(conf);
            this.actionType = Key.getActionType(conf);
            this.urlParams = Key.getUrlParams(conf);
            this.enableWriteNull = Key.isEnableNullUpdate(conf);
            this.retryTimes = this.conf.getInt("retryTimes", 3);
            this.sleepTimeInMilliSecond = this.conf.getLong("sleepTimeInMilliSecond", 10000L);
            this.isGreaterOrEqualThan7 = this.conf.getBool("isGreaterOrEqualThan7", false);
            this.parseDeleteCondition(conf);
            this.columnList = JSON.parseObject(this.conf.getString(WRITE_COLUMNS), new TypeReference<List<ElasticSearchColumn>>() {
            });
            LOGGER.info("columnList: {}", JSON.toJSONString(columnList));
            this.hasId = this.conf.getBool("hasId", false);
            if (hasId) {
                LOGGER.info("Task has id column, will use it to set _id property");
            } else {
                LOGGER.info("Task will use elasticsearch auto generated _id property");
            }
            this.fieldDelimiter = Key.getFieldDelimiter(this.conf);
            this.enableRedundantColumn = this.conf.getBool("enableRedundantColumn", false);
            this.typeList = new ArrayList<ElasticSearchFieldType>();
            for (ElasticSearchColumn esColumn : this.columnList) {
                this.typeList.add(ElasticSearchFieldType.getESFieldType(esColumn.getType()));
                if (esColumn.getCombineFields() != null && esColumn.getCombineFields().size() > 0
                    && ElasticSearchFieldType.getESFieldType(esColumn.getType()).equals(ElasticSearchFieldType.ID)) {
                    combinedIdColumn = esColumn;
                }
            }
            this.primaryKeyInfo = Key.getPrimaryKeyInfo(this.conf);
            this.esPartitionColumn = Key.getEsPartitionColumn(this.conf);
            this.colNameToIndexMap = new HashMap<String, Integer>(5);
            this.handleMetaKeys();
            this.esClient = new ElasticSearchClient(this.conf);
        }
        
        private void handleMetaKeys() {
            if (null != this.primaryKeyInfo && null != this.primaryKeyInfo.getColumn()
                    && !this.primaryKeyInfo.getColumn().isEmpty()) {
                this.hasPrimaryKeyInfo = true;
                if (null == this.primaryKeyInfo.getFieldDelimiter()) {
                    if (null != this.fieldDelimiter) {
                        this.primaryKeyInfo.setFieldDelimiter(this.fieldDelimiter);
                    } else {
                        this.primaryKeyInfo.setFieldDelimiter("");
                    }
                }
               
                for (String eachPk : this.primaryKeyInfo.getColumn()) {
                    boolean foundKeyInColumn = false;
                    for (int i = 0; i < columnList.size(); i++) {
                        if (StringUtils.equals(eachPk, columnList.get(i).getName())) {
                            this.colNameToIndexMap.put(eachPk, i);
                            foundKeyInColumn = true;
                            break;
                        }
                    }
                    if (!foundKeyInColumn) {
                        throw DataXException.asDataXException(ElasticSearchWriterErrorCode.RECORD_FIELD_NOT_FOUND,
                                "primaryKeyInfo has column not exists in column");
                    }
                }
            }

            if (null != this.esPartitionColumn && !this.esPartitionColumn.isEmpty()) {
                this.hasEsPartitionColumn = true;
                for (PartitionColumn eachPartitionCol : this.esPartitionColumn) {
                    boolean foundKeyInColumn = false;
                    for (int i = 0; i < columnList.size(); i++) {
                        if (StringUtils.equals(eachPartitionCol.getName(), columnList.get(i).getName())) {
                            this.colNameToIndexMap.put(eachPartitionCol.getName(), i);
                            foundKeyInColumn = true;
                            break;
                        }
                    }
                    if (!foundKeyInColumn) {
                        throw DataXException.asDataXException(ElasticSearchWriterErrorCode.RECORD_FIELD_NOT_FOUND,
                                "esPartitionColumn has column not exists in column");
                    }
                }
            }
        }

        private void parseDeleteCondition(Configuration conf) {
            List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
            String config = Key.getDeleteBy(conf);
            if (config != null) {
                JSONArray array = JSON.parseArray(config);
                for (Object obj : array) {
                    list.add((Map) obj);
                }
                deleteByConditions = list;
            }
        }


        @Override
        public void prepare() {
        }

        /**
         * 示例：{
         *          "deleteBy" : [
         *              {"product_status" : [-1,-2], "sub_status" : -3}，
         *              {"product_status" : -3}
         *          ]
         *      }
         *
         *  表示以下两类数据删除：
         *  1.  product_status为-1或-2并且sub_status为-3
         *  2.  product_status为-3
         *
         *  注意[{}]返回true
         * @param record
         * @return
         */
        private boolean isDeleteRecord(Record record) {
            if (deleteByConditions == null) {
                return false;
            }

            Map<String, Object> kv = new HashMap<String, Object>();
            for (int i = 0; i < record.getColumnNumber(); i++) {
                Column column = record.getColumn(i);
                String columnName = columnList.get(i).getName();
                kv.put(columnName, column.asString());
            }

            for (Map<String, Object> delCondition : deleteByConditions) {
                if (meetAllCondition(kv, delCondition)) {
                    return true;
                }
            }

            return false;
        }

        private boolean meetAllCondition(Map<String, Object> kv, Map<String, Object> delCondition) {
            for (Map.Entry<String, Object> oneCondition : delCondition.entrySet()) {
                if (!checkOneCondition(kv, oneCondition)) {
                    return false;
                }
            }
            return true;
        }

        private boolean checkOneCondition(Map<String, Object> kv, Map.Entry<String, Object> entry) {
            Object value = kv.get(entry.getKey());
            if (entry.getValue() instanceof List) {
                for (Object obj : (List) entry.getValue()) {
                    if (obj.toString().equals(value)) {
                        return true;
                    }
                }
            } else {
                if (value != null && value.equals(entry.getValue().toString())) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            List<Record> writerBuffer = new ArrayList<Record>(this.batchSize);
            Record record = null;
            while ((record = recordReceiver.getFromReader()) != null) {
                if (!columnSizeChecked) {
                    boolean isInvalid = true;
                    if (enableRedundantColumn) {
                        isInvalid = this.columnList.size() > record.getColumnNumber();
                    } else {
                        isInvalid = this.columnList.size() != record.getColumnNumber();
                    }
                    if (isInvalid) {
                        String message = String.format(
                                "column number not equal error, reader column size is %s, but the writer column size is %s",
                                record.getColumnNumber(), this.columnList.size());
                        throw DataXException.asDataXException(ElasticSearchWriterErrorCode.BAD_CONFIG_VALUE, message);
                    }
                    columnSizeChecked = true;
                }
                writerBuffer.add(record);
                if (writerBuffer.size() >= this.batchSize) {
                    this.doBatchInsert(writerBuffer);
                    writerBuffer.clear();
                }
            }

            if (!writerBuffer.isEmpty()) {
                this.doBatchInsert(writerBuffer);
                writerBuffer.clear();
            }
        }

        private String getDateStr(ElasticSearchColumn esColumn, Column column) {
            // 如果保持原样，就直接返回
            if (esColumn.isOrigin()) {
                return column.asString();
            }
            DateTime date = null;
            DateTimeZone dtz = DateTimeZone.getDefault();
            if (esColumn.getTimezone() != null) {
                // 所有时区参考 http://www.joda.org/joda-time/timezones.html
                // TODO：创建一次多处复用
                dtz = DateTimeZone.forID(esColumn.getTimezone());
            }
            if (column.getType() != Column.Type.DATE && esColumn.getFormat() != null) {
                // TODO：创建一次多处复用
                DateTimeFormatter formatter = DateTimeFormat.forPattern(esColumn.getFormat());
                date = formatter.withZone(dtz).parseDateTime(column.asString());
                return date.toString();
            } else if (column.getType() == Column.Type.DATE) {
                if (null == column.getRawData()) {
                    return null;
                } else {
                    date = new DateTime(column.asLong(), dtz);
                    return date.toString();
                }
            } else {
                return column.asString();
            }
        }

        private void doBatchInsert(final List<Record> writerBuffer) {
            Map<String, Object> data = null;
            Bulk.Builder bulkactionTmp = null;
            int totalNumber = writerBuffer.size();
            int dirtyDataNumber = 0;
            if (this.isGreaterOrEqualThan7) {
                bulkactionTmp = new Bulk.Builder().defaultIndex(this.index);
            } else {
                bulkactionTmp = new Bulk.Builder().defaultIndex(this.index).defaultType(this.type);
            }
            final Bulk.Builder bulkaction = bulkactionTmp;
            // 增加url的参数
            for (Map.Entry<String, Object> entry : urlParams.entrySet()) {
                bulkaction.setParameter(entry.getKey(), entry.getValue());
            }
            for (Record record : writerBuffer) {
                data = new HashMap<String, Object>();
                String id = null;
                String parent = null;
                String routing = null;
                String version = null;
                String columnName = null;
                Column column = null;
                try {
                    for (int i = 0; i < record.getColumnNumber(); i++) {
                        column = record.getColumn(i);
                        columnName = columnList.get(i).getName();
                        // 如果组合id不等于null，需要把相关的字段全部忽略
                        if (combinedIdColumn != null) {
                            if (combinedIdColumn.getCombineFields().contains(columnName)) {
                                continue;
                            }
                        }
                        //如果是json数组，当成对象类型处理
                        ElasticSearchFieldType columnType = columnList.get(i).isJsonArray() ? ElasticSearchFieldType.NESTED : typeList.get(i);

                        Boolean dstArray = columnList.get(i).isDstArray();

                        //如果是数组类型，那它传入的是字符串类型，也有可能是null
                        if (columnList.get(i).isArray() && null != column.asString()) {
                            String[] dataList = column.asString().split(splitter);
                            if (!columnType.equals(ElasticSearchFieldType.DATE)) {
                                if (dstArray) {
                                    try {
                                        // 根据客户配置的类型，转换成相应的类型
                                        switch (columnType) {
                                            case BYTE:
                                            case KEYWORD:
                                            case TEXT:
                                                data.put(columnName, dataList);
                                                break;
                                            case SHORT:
                                            case INTEGER:
                                                if (StringUtils.isBlank(column.asString().trim())) {
                                                    data.put(columnName, null);
                                                } else {
                                                    Integer[] intDataList = new Integer[dataList.length];
                                                    for (int j = 0; j < dataList.length; j++) {
                                                        dataList[j] = dataList[j].trim();
                                                        if (StringUtils.isNotBlank(dataList[j])) {
                                                            intDataList[j] = Integer.valueOf(dataList[j]);
                                                        }
                                                    }
                                                    data.put(columnName, intDataList);
                                                }
                                                break;
                                            case LONG:
                                                if (StringUtils.isBlank(column.asString().trim())) {
                                                    data.put(columnName, null);
                                                } else {
                                                    Long[] longDataList = new Long[dataList.length];
                                                    for (int j = 0; j < dataList.length; j++) {
                                                        dataList[j] = dataList[j].trim();
                                                        if (StringUtils.isNotBlank(dataList[j])) {
                                                            longDataList[j] = Long.valueOf(dataList[j]);
                                                        }
                                                    }
                                                    data.put(columnName, longDataList);
                                                }
                                                break;
                                            case FLOAT:
                                            case DOUBLE:
                                                if (StringUtils.isBlank(column.asString().trim())) {
                                                    data.put(columnName, null);
                                                } else {
                                                    Double[] doubleDataList = new Double[dataList.length];
                                                    for (int j = 0; j < dataList.length; j++) {
                                                        dataList[j] = dataList[j].trim();
                                                        if (StringUtils.isNotBlank(dataList[j])) {
                                                            doubleDataList[j] = Double.valueOf(dataList[j]);
                                                        }
                                                    }
                                                    data.put(columnName, doubleDataList);
                                                }
                                                break;
                                            default:
                                                data.put(columnName, dataList);
                                                break;
                                        }
                                    } catch (Exception e) {
                                        LOGGER.info("脏数据，记录:{}", record.toString());
                                        continue;
                                    }
                                } else {
                                    data.put(columnName, dataList);
                                }
                            } else {
                                data.put(columnName, dataList);
                            }
                        } else {
                            // LOGGER.info("columnType: {} integer: {}", columnType, column.asString());
                            switch (columnType) {
                                case ID:
                                    if (id != null) {
                                        id += record.getColumn(i).asString();
                                    } else {
                                        id = record.getColumn(i).asString();
                                    }
                                    break;
                                case PARENT:
                                    if (parent != null) {
                                        parent += record.getColumn(i).asString();
                                    } else {
                                        parent = record.getColumn(i).asString();
                                    }
                                    break;
                                case ROUTING:
                                    if (routing != null) {
                                        routing += record.getColumn(i).asString();
                                    } else {
                                        routing = record.getColumn(i).asString();
                                    }
                                    break;

                                case VERSION:
                                    if (version != null) {
                                        version += record.getColumn(i).asString();
                                    } else {
                                        version = record.getColumn(i).asString();
                                    }
                                    break;
                                case DATE:
                                    String dateStr = getDateStr(columnList.get(i), column);
                                    data.put(columnName, dateStr);
                                    break;
                                case KEYWORD:
                                case STRING:
                                case TEXT:
                                case IP:
                                case GEO_POINT:
                                case IP_RANGE:
                                    data.put(columnName, column.asString());
                                    break;
                                case BOOLEAN:
                                    data.put(columnName, column.asBoolean());
                                    break;
                                case BYTE:
                                case BINARY:
                                    // json序列化不支持byte类型，es支持的binary类型，必须传入base64的格式
                                    data.put(columnName, column.asString());
                                    break;
                                case LONG:
                                    data.put(columnName, column.asLong());
                                    break;
                                case INTEGER:
                                    data.put(columnName, column.asLong());
                                    break;
                                case SHORT:
                                    data.put(columnName, column.asLong());
                                    break;
                                case FLOAT:
                                case DOUBLE:
                                    data.put(columnName, column.asDouble());
                                    break;
                                case GEO_SHAPE:
                                case DATE_RANGE:
                                case INTEGER_RANGE:
                                case FLOAT_RANGE:
                                case LONG_RANGE:
                                case DOUBLE_RANGE:
                                    if (null == column.asString()) {
                                        data.put(columnName, column.asString());
                                    } else {
                                        data.put(columnName, JSON.parse(column.asString()));
                                    }
                                    break;
                                case NESTED:
                                case OBJECT:
                                    if (null == column.asString()) {
                                        data.put(columnName, column.asString());
                                    } else {
                                        // 转json格式
                                        data.put(columnName, JSON.parse(column.asString()));
                                    }
                                    break;
                                default:
                                throw DataXException.asDataXException(ElasticSearchWriterErrorCode.BAD_CONFIG_VALUE, String.format(
                                        "Type error: unsupported type %s for column %s", columnType, columnName));
                            }
                        }
                    }
                    
                    
                    if (this.hasPrimaryKeyInfo) {
                        List<String> idData = new ArrayList<String>();
                        for (String eachCol : this.primaryKeyInfo.getColumn()) {
                            Column recordColumn = record.getColumn(this.colNameToIndexMap.get(eachCol));
                            idData.add(recordColumn.asString());
                        }
                        id = StringUtils.join(idData, this.primaryKeyInfo.getFieldDelimiter());
                    }
                    if (this.hasEsPartitionColumn) {
                        List<String> idData = new ArrayList<String>();
                        for (PartitionColumn eachCol : this.esPartitionColumn) {
                            Column recordColumn = record.getColumn(this.colNameToIndexMap.get(eachCol.getName()));
                            idData.add(recordColumn.asString());
                        }
                        routing = StringUtils.join(idData, "");
                    }
                } catch (Exception e) {
                    // 脏数据
                    super.getTaskPluginCollector().collectDirtyRecord(record,
                            String.format("parse error for column: %s errorMessage: %s", columnName, e.getMessage()));
                    dirtyDataNumber++;
                    // 处理下一个record
                    continue;
                }
                
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("id: {} routing: {} data: {}", id, routing, JSON.toJSONString(data));
                }
                

                if (isDeleteRecord(record)) {
                    Delete.Builder builder = new Delete.Builder(id);
                    bulkaction.addAction(builder.build());
                } else {
                    // 使用用户自定义组合唯一键
                    if (combinedIdColumn != null) {
                        try {
                            id = processIDCombineFields(record, combinedIdColumn);
                            // LOGGER.debug("id: {}", id);
                        } catch (Exception e) {
                            // 脏数据
                            super.getTaskPluginCollector().collectDirtyRecord(record,
                                    String.format("parse error for column: %s errorMessage: %s", columnName, e.getMessage()));
                            // 处理下一个record
                            dirtyDataNumber++;
                            continue;
                        }
                    }
                    switch (actionType) {
                        case INDEX:
                            // 先进行json序列化，jest client的gson序列化会把等号按照html序列化
                            Index.Builder builder = null;
                            if (this.enableWriteNull) {
                                builder = new Index.Builder(
                                        JSONObject.toJSONString(data, JSONWriter.Feature.WriteMapNullValue,
                                                JSONWriter.Feature.WriteEnumUsingToString));
                            } else {
                                builder = new Index.Builder(JSONObject.toJSONString(data));
                            }
                            if (id != null) {
                                builder.id(id);
                            }
                            if (parent != null) {
                                builder.setParameter(Parameters.PARENT, parent);
                            }
                            if (routing != null) {
                                builder.setParameter(Parameters.ROUTING, routing);
                            }
                            if (version != null) {
                                builder.setParameter(Parameters.VERSION, version);
                                builder.setParameter(Parameters.VERSION_TYPE, "external");
                            }
                            bulkaction.addAction(builder.build());
                            break;
                        case UPDATE:
                            // doc: https://www.cnblogs.com/crystaltu/articles/6992935.html
                            // doc: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html
                            Map<String, Object> updateDoc = new HashMap<String, Object>();
                            updateDoc.put("doc", data);
                            updateDoc.put("doc_as_upsert", true);
                            Update.Builder update = null;
                            if (this.enableWriteNull) {
                                // write: {a:"1",b:null}
                            update = new Update.Builder(
                                    JSONObject.toJSONString(updateDoc, JSONWriter.Feature.WriteMapNullValue,
                                            JSONWriter.Feature.WriteEnumUsingToString));
                            // 在DEFAULT_GENERATE_FEATURE基础上，只增加了SerializerFeature.WRITE_MAP_NULL_FEATURES
                            } else {
                                // write: {"a":"1"}
                                update = new Update.Builder(JSONObject.toJSONString(updateDoc));
                            }
                            if (id != null) {
                                update.id(id);
                            }
                            if (parent != null) {
                                update.setParameter(Parameters.PARENT, parent);
                            }
                            if (routing != null) {
                                update.setParameter(Parameters.ROUTING, routing);
                            }
                            // version type [EXTERNAL] is not supported by the update API
                            if (version != null) {
                                update.setParameter(Parameters.VERSION, version);
                            }
                            bulkaction.addAction(update.build());
                            break;
                        default:
                            break;
                    }
                }
            }
            
            if (dirtyDataNumber >= totalNumber) {
                // all batch is dirty data
                LOGGER.warn("all this batch is dirty data, dirtyDataNumber: {} totalDataNumber: {}", dirtyDataNumber,
                        totalNumber);
                return;
            }
            
            BulkResult bulkResult = null;
            try {
                bulkResult = RetryUtil.executeWithRetry(new Callable<BulkResult>() {
                    @Override
                    public BulkResult call() throws Exception {
                        JestResult jestResult = esClient.bulkInsert(bulkaction);
                        if (jestResult.isSucceeded()) {
                            return null;
                        }
                        String msg = String.format("response code: [%d] error :[%s]", jestResult.getResponseCode(),
                                jestResult.getErrorMessage());
                        LOGGER.warn(msg);
                        if (esClient.isBulkResult(jestResult)) {
                            BulkResult brst = (BulkResult) jestResult;
                            List<BulkResult.BulkResultItem> failedItems = brst.getFailedItems();
                            for (BulkResult.BulkResultItem item : failedItems) {
                                if (item.status != 400) {
                                    // 400 BAD_REQUEST 如果非数据异常,请求异常,则不允许忽略
                                    throw DataXException.asDataXException(ElasticSearchWriterErrorCode.ES_INDEX_INSERT,
                                            String.format("status:[%d], error: %s", item.status, item.error));
                                } else {
                                    // 如果用户选择不忽略解析错误,则抛异常,默认为忽略
                                    if (!Key.isIgnoreParseError(conf)) {
                                        throw new NoReRunException(ElasticSearchWriterErrorCode.ES_INDEX_INSERT,
                                                String.format(
                                                        "status:[%d], error: %s, config not ignoreParseError so throw this error",
                                                        item.status, item.error));
                                    }
                                }
                            }
                            return brst;
                        } else {
                            Integer status = esClient.getStatus(jestResult);
                            switch (status) {
                            case 429: // TOO_MANY_REQUESTS
                                LOGGER.warn("server response too many requests, so auto reduce speed");
                                break;
                            default:
                                break;
                            }
                            throw DataXException.asDataXException(ElasticSearchWriterErrorCode.ES_INDEX_INSERT,
                                    jestResult.getErrorMessage());
                        }
                    }
                }, this.trySize, this.tryInterval, false, Arrays.asList(DataXException.class));
            } catch (Exception e) {
                if (Key.isIgnoreWriteError(this.conf)) {
                    LOGGER.warn(String.format("Retry [%d] write failed, ignore the error, continue to write!", trySize));
                } else {
                    throw DataXException.asDataXException(ElasticSearchWriterErrorCode.ES_INDEX_INSERT, e.getMessage(), e);
                }
            }
            
            if (null != bulkResult) {
                List<BulkResult.BulkResultItem> items = bulkResult.getItems();
                for (int idx = 0; idx < items.size(); ++idx) {
                    BulkResult.BulkResultItem item = items.get(idx);
                    if (item.error != null && !"".equals(item.error)) {
                        super.getTaskPluginCollector().collectDirtyRecord(writerBuffer.get(idx),
                                String.format("status:[%d], error: %s", item.status, item.error));
                    }
                }
            }
        }

        private int getRecordColumnIndex(Record record, String columnName) {
            if (colNameToIndexMap.containsKey(columnName)) {
                return colNameToIndexMap.get(columnName);
            }

            List<Column> columns = new ArrayList<Column>();
            int index = -1;
            for (int i=0; i<record.getColumnNumber(); i++) {
                Column column = record.getColumn(i);
                String colName = columnList.get(i).getName();
                if (StringUtils.isNotBlank(columnName) && colName.equals(columnName)) {
                    columns.add(column);
                    index = i;
                }
            }

            if (columns == null || columns.isEmpty()) {
                throw DataXException.asDataXException(ElasticSearchWriterErrorCode.RECORD_FIELD_NOT_FOUND, columnName);
            }

            if (columns != null && columns.size() > 1) {
                throw DataXException.asDataXException(
                    ElasticSearchWriterErrorCode.RECORD_FIELD_NOT_FOUND,
                    "record has multiple columns found by name: " + columnName);
            }

            colNameToIndexMap.put(columnName, index);
            return index;
        }

        private String processIDCombineFields(Record record, ElasticSearchColumn esColumn) {
            List<String> values = new ArrayList<String>(esColumn.getCombineFields().size());
            for (String field : esColumn.getCombineFields()) {
                int colIndex = getRecordColumnIndex(record, field);
                Column col = record.getColumnNumber() <= colIndex ? null : record.getColumn(colIndex);
                if (col == null) {
                    throw DataXException.asDataXException(ElasticSearchWriterErrorCode.RECORD_FIELD_NOT_FOUND, field);
                }
                values.add(col.asString());
            }
            return Joiner.on(esColumn.getCombineFieldsValueSeparator()).join(values);
        }
        
        @Override
        public void post() {
        }

        @Override
        public void destroy() {
            try {
                this.esClient.closeJestClient();
            } catch (Exception e) {
                LOGGER.warn("ignore close jest client error: {}", e.getMessage());
            }
        }
        
    }
}
