package com.alibaba.datax.plugin.writer.datahubwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.DataXCaseEnvUtil;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.fastjson2.JSON;
import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.model.FieldType;
import com.aliyun.datahub.client.model.GetTopicResult;
import com.aliyun.datahub.client.model.ListShardResult;
import com.aliyun.datahub.client.model.PutErrorEntry;
import com.aliyun.datahub.client.model.PutRecordsResult;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.RecordType;
import com.aliyun.datahub.client.model.ShardEntry;
import com.aliyun.datahub.client.model.ShardState;
import com.aliyun.datahub.client.model.TupleRecordData;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

public class DatahubWriter extends Writer {

    /**
     * Job 中的方法仅执行一次，Task 中方法会由框架启动多个 Task 线程并行执行。
     * <p/>
     * 整个 Writer 执行流程是：
     * <pre>
     * Job类init-->prepare-->split
     *
     *                          Task类init-->prepare-->startWrite-->post-->destroy
     *                          Task类init-->prepare-->startWrite-->post-->destroy
     *
     *                                                                            Job类post-->destroy
     * </pre>
     */
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private Configuration jobConfig = null;

        @Override
        public void init() {
            this.jobConfig = super.getPluginJobConf();
            jobConfig.getNecessaryValue(Key.CONFIG_KEY_ENDPOINT, DatahubWriterErrorCode.MISSING_REQUIRED_VALUE);
            jobConfig.getNecessaryValue(Key.CONFIG_KEY_ACCESS_ID, DatahubWriterErrorCode.MISSING_REQUIRED_VALUE);
            jobConfig.getNecessaryValue(Key.CONFIG_KEY_ACCESS_KEY, DatahubWriterErrorCode.MISSING_REQUIRED_VALUE);
            jobConfig.getNecessaryValue(Key.CONFIG_KEY_PROJECT, DatahubWriterErrorCode.MISSING_REQUIRED_VALUE);
            jobConfig.getNecessaryValue(Key.CONFIG_KEY_TOPIC, DatahubWriterErrorCode.MISSING_REQUIRED_VALUE);
        }
        
        @Override
        public void prepare() {
            String project = jobConfig.getNecessaryValue(Key.CONFIG_KEY_PROJECT,
                    DatahubWriterErrorCode.MISSING_REQUIRED_VALUE);
            String topic = jobConfig.getNecessaryValue(Key.CONFIG_KEY_TOPIC,
                    DatahubWriterErrorCode.MISSING_REQUIRED_VALUE);
            RecordType recordType = null;
            DatahubClient client = DatahubClientHelper.getDatahubClient(this.jobConfig);
            try {
                GetTopicResult getTopicResult = client.getTopic(project, topic);
                recordType = getTopicResult.getRecordType();
            } catch (Exception e) {
                LOG.warn("get topic type error: {}", e.getMessage());
            }
            if (null != recordType) {
                if (recordType == RecordType.BLOB) {
                    throw DataXException.asDataXException(DatahubWriterErrorCode.WRITE_DATAHUB_FAIL,
                            "DatahubWriter only support 'Tuple' RecordType now, but your RecordType is 'BLOB'");
                }
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configs = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; ++i) {
                configs.add(jobConfig.clone());
            }
            return configs;
        }

        @Override
        public void post() {}

        @Override
        public void destroy() {}

    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);
        private static final List<String> FATAL_ERRORS_DEFAULT = Arrays.asList(
                "InvalidParameterM",
                "MalformedRecord",
                "INVALID_SHARDID",
                "NoSuchTopic",
                "NoSuchShard"
        );

        private Configuration taskConfig;
        private DatahubClient client;
        private String project;
        private String topic;
        private List<String> shards;
        private int maxCommitSize;
        private int maxRetryCount;
        private RecordSchema schema;
        private long retryInterval;
        private Random random;
        private List<String> column;
        private List<Integer> columnIndex;
        private boolean enableColumnConfig;
        private List<String> fatalErrors;
        
        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();
            project = taskConfig.getNecessaryValue(Key.CONFIG_KEY_PROJECT, DatahubWriterErrorCode.MISSING_REQUIRED_VALUE);
            topic = taskConfig.getNecessaryValue(Key.CONFIG_KEY_TOPIC, DatahubWriterErrorCode.MISSING_REQUIRED_VALUE);
            maxCommitSize = taskConfig.getInt(Key.CONFIG_KEY_MAX_COMMIT_SIZE, 1024*1024);
            maxRetryCount = taskConfig.getInt(Key.CONFIG_KEY_MAX_RETRY_COUNT, 500);
            this.retryInterval = taskConfig.getInt(Key.RETRY_INTERVAL, 650);
            this.random = new Random();
            this.column = this.taskConfig.getList(Key.CONFIG_KEY_COLUMN, String.class);
            // ["*"]
            if (null != this.column && 1 == this.column.size()) {
                if (StringUtils.equals("*", this.column.get(0))) {
                    this.column = null;
                }
            } 
            this.columnIndex = new ArrayList<Integer>();
            // 留个开关保平安
            this.enableColumnConfig = this.taskConfig.getBool("enableColumnConfig", true);
            this.fatalErrors = this.taskConfig.getList("fatalErrors", Task.FATAL_ERRORS_DEFAULT, String.class);
            this.client = DatahubClientHelper.getDatahubClient(this.taskConfig);
        }

        @Override
        public void prepare() {
            final String shardIdConfig = this.taskConfig.getString(Key.CONFIG_KEY_SHARD_ID);
            this.shards = new ArrayList<String>();
            try {
                RetryUtil.executeWithRetry(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        ListShardResult result = client.listShard(project, topic);
                        if (StringUtils.isNotBlank(shardIdConfig)) {
                            shards.add(shardIdConfig);
                        } else {
                            for (ShardEntry shard : result.getShards()) {
                                if (shard.getState() == ShardState.ACTIVE || shard.getState() == ShardState.OPENING) {
                                    shards.add(shard.getShardId());
                                }
                            }
                        }
                        schema = client.getTopic(project, topic).getRecordSchema();
                        return null;
                    }
                }, DataXCaseEnvUtil.getRetryTimes(5), DataXCaseEnvUtil.getRetryInterval(10000L), DataXCaseEnvUtil.getRetryExponential(false));
                } catch (Exception e) {
                    throw DataXException.asDataXException(DatahubWriterErrorCode.GET_TOPOIC_INFO_FAIL,
                            "get topic info failed", e);
                }
            LOG.info("datahub topic {} shard to write: {}", this.topic, JSON.toJSONString(this.shards));
            LOG.info("datahub topic {} has schema: {}", this.topic, JSON.toJSONString(this.schema));
            
            // 根据 schmea 顺序 和用户配置的 column，计算写datahub的顺序关系，以支持列换序
            // 后续统一使用 columnIndex 的顺位关系写 datahub
            int totalSize = this.schema.getFields().size();
            if (null != this.column && !this.column.isEmpty() && this.enableColumnConfig) {
                for (String eachCol : this.column) {
                    int indexFound = -1;
                    for (int i = 0; i < totalSize; i++) {
                        // warn: 大小写ignore
                        if (StringUtils.equalsIgnoreCase(eachCol, this.schema.getField(i).getName())) {
                            indexFound = i;
                            break;
                        }
                    }
                    if (indexFound >= 0) {
                        this.columnIndex.add(indexFound);
                    } else {
                        throw DataXException.asDataXException(DatahubWriterErrorCode.SCHEMA_NOT_MATCH,
                                String.format("can not find column %s in datahub topic %s", eachCol, this.topic));
                    }
                }
            } else {
                for (int i = 0; i < totalSize; i++) {
                    this.columnIndex.add(i);
                }
            }
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            Record record;
            List<RecordEntry> records = new ArrayList<RecordEntry>();
            String shardId = null;
            if (1 == this.shards.size()) {
                shardId = shards.get(0);
            } else {
                shardId = shards.get(this.random.nextInt(shards.size()));
            }
            int commitSize = 0;
            try {
                while ((record = recordReceiver.getFromReader()) != null) {
                    RecordEntry dhRecord = convertRecord(record, shardId);
                    if (dhRecord != null) {
                        records.add(dhRecord);
                    }
                    commitSize += record.getByteSize();
                    if (commitSize >= maxCommitSize) {
                        commit(records);
                        records.clear();
                        commitSize = 0;
                        if (1 == this.shards.size()) {
                            shardId = shards.get(0);
                        } else {
                            shardId = shards.get(this.random.nextInt(shards.size()));
                        }
                    }
                }
                if (commitSize > 0) {
                    commit(records);
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DatahubWriterErrorCode.WRITE_DATAHUB_FAIL, e);
            }
        }

        @Override
        public void post() {}

        @Override
        public void destroy() {}

        private void commit(List<RecordEntry> records) throws InterruptedException {
            PutRecordsResult result = client.putRecords(project, topic, records);
            if (result.getFailedRecordCount() > 0) {
                for (int i = 0; i < maxRetryCount; ++i) {
                    boolean limitExceededMessagePrinted = false;
                    for (PutErrorEntry error : result.getPutErrorEntries()) {
                        // 如果是 LimitExceeded 这样打印日志，不能每行记录打印一次了
                        if (StringUtils.equalsIgnoreCase("LimitExceeded", error.getErrorcode())) {
                            if (!limitExceededMessagePrinted) {
                                LOG.warn("write record error, request id: {}, error code: {}, error message: {}",
                                        result.getRequestId(), error.getErrorcode(), error.getMessage());
                                limitExceededMessagePrinted = true;
                            }
                        } else {
                            LOG.error("write record error, request id: {}, error code: {}, error message: {}",
                                    result.getRequestId(), error.getErrorcode(), error.getMessage());
                        }
                        if (this.fatalErrors.contains(error.getErrorcode())) {
                            throw DataXException.asDataXException(
                                    DatahubWriterErrorCode.WRITE_DATAHUB_FAIL,
                                    error.getMessage());
                        }
                    }
                    
                    if (this.retryInterval >= 0) {
                        Thread.sleep(this.retryInterval);
                    } else {
                        Thread.sleep(new Random().nextInt(700) + 300);
                    }
                   
                    result = client.putRecords(project, topic, result.getFailedRecords());
                    if (result.getFailedRecordCount() == 0) {
                        return;
                    }
                }
                throw DataXException.asDataXException(
                        DatahubWriterErrorCode.WRITE_DATAHUB_FAIL,
                        "write datahub failed");
            }
        }

        private RecordEntry convertRecord(Record dxRecord, String shardId) {
            try {
                RecordEntry dhRecord = new RecordEntry();
                dhRecord.setShardId(shardId);
                TupleRecordData data = new TupleRecordData(this.schema);
                for (int i = 0; i < this.columnIndex.size(); ++i) {
                    int orderInSchema = this.columnIndex.get(i);
                    FieldType type = this.schema.getField(orderInSchema).getType();
                    Column column = dxRecord.getColumn(i);
                    switch (type) {
                        case BIGINT:
                            data.setField(orderInSchema, column.asLong());
                            break;
                        case DOUBLE:
                            data.setField(orderInSchema, column.asDouble());
                            break;
                        case STRING:
                            data.setField(orderInSchema, column.asString());
                            break;
                        case BOOLEAN:
                            data.setField(orderInSchema, column.asBoolean());
                            break;
                        case TIMESTAMP:
                            if (null == column.asDate()) {
                                data.setField(orderInSchema, null);
                            } else {
                                data.setField(orderInSchema, column.asDate().getTime() * 1000);
                            }
                            break;
                        case DECIMAL:
                            // warn
                            data.setField(orderInSchema, column.asBigDecimal());
                            break;
                        case INTEGER:
                            data.setField(orderInSchema, column.asLong());
                            break;
                        case FLOAT:
                            data.setField(orderInSchema, column.asDouble());
                            break;
                        case TINYINT:
                            data.setField(orderInSchema, column.asLong());
                            break;
                        case SMALLINT:
                            data.setField(orderInSchema, column.asLong());
                            break;
                        default:
                            throw DataXException.asDataXException(
                                    DatahubWriterErrorCode.SCHEMA_NOT_MATCH,
                                    String.format("does not support type: %s", type));
                    }
                }
                dhRecord.setRecordData(data);
                return dhRecord;
            } catch (Exception e) {
                super.getTaskPluginCollector().collectDirtyRecord(dxRecord, e, "convert recor failed");
            }
            return null;
        }
    }

}