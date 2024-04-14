package com.alibaba.datax.plugin.reader.datahubreader;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.aliyun.datahub.client.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;


import com.aliyun.datahub.client.DatahubClient;


public class DatahubReader extends Reader {
    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        
        private Configuration originalConfig;
        
        private Long beginTimestampMillis;
        private Long endTimestampMillis;
        
        DatahubClient datahubClient;
        
        @Override
        public void init() {
            LOG.info("datahub reader job init begin ...");
            this.originalConfig = super.getPluginJobConf();
            validateParameter(originalConfig);
            this.datahubClient = DatahubClientHelper.getDatahubClient(this.originalConfig);
            LOG.info("datahub reader job init end.");
        }
        
        private void validateParameter(Configuration conf){
            conf.getNecessaryValue(Key.ENDPOINT,DatahubReaderErrorCode.REQUIRE_VALUE);
            conf.getNecessaryValue(Key.ACCESSKEYID,DatahubReaderErrorCode.REQUIRE_VALUE);
            conf.getNecessaryValue(Key.ACCESSKEYSECRET,DatahubReaderErrorCode.REQUIRE_VALUE);
            conf.getNecessaryValue(Key.PROJECT,DatahubReaderErrorCode.REQUIRE_VALUE);
            conf.getNecessaryValue(Key.TOPIC,DatahubReaderErrorCode.REQUIRE_VALUE);
            conf.getNecessaryValue(Key.COLUMN,DatahubReaderErrorCode.REQUIRE_VALUE);
            conf.getNecessaryValue(Key.BEGINDATETIME,DatahubReaderErrorCode.REQUIRE_VALUE);
            conf.getNecessaryValue(Key.ENDDATETIME,DatahubReaderErrorCode.REQUIRE_VALUE);
            
            int batchSize = this.originalConfig.getInt(Key.BATCHSIZE, 1024);
            if (batchSize > 10000) {
                throw DataXException.asDataXException(DatahubReaderErrorCode.BAD_CONFIG_VALUE,
                        "Invalid batchSize[" + batchSize + "] value (0,10000]!");
            }
            
            String beginDateTime = this.originalConfig.getString(Key.BEGINDATETIME);            
            if (beginDateTime != null) {
                try {
                    beginTimestampMillis = DatahubReaderUtils.getUnixTimeFromDateTime(beginDateTime);
                } catch (ParseException e) {
                    throw DataXException.asDataXException(DatahubReaderErrorCode.BAD_CONFIG_VALUE,
                            "Invalid beginDateTime[" + beginDateTime + "], format [yyyyMMddHHmmss]!");    
                }
            }
            
            if (beginTimestampMillis != null && beginTimestampMillis <= 0) {
                throw DataXException.asDataXException(DatahubReaderErrorCode.BAD_CONFIG_VALUE,
                        "Invalid beginTimestampMillis[" + beginTimestampMillis + "]!");               
            }
            
            String endDateTime = this.originalConfig.getString(Key.ENDDATETIME);            
            if (endDateTime != null) {
                try {
                    endTimestampMillis = DatahubReaderUtils.getUnixTimeFromDateTime(endDateTime);
                } catch (ParseException e) {
                    throw DataXException.asDataXException(DatahubReaderErrorCode.BAD_CONFIG_VALUE,
                            "Invalid beginDateTime[" + endDateTime + "], format [yyyyMMddHHmmss]!");    
                }
            }
            
            if (endTimestampMillis != null && endTimestampMillis <= 0) {
                throw DataXException.asDataXException(DatahubReaderErrorCode.BAD_CONFIG_VALUE,
                        "Invalid endTimestampMillis[" + endTimestampMillis + "]!");                
            }
            
            if (beginTimestampMillis != null && endTimestampMillis != null
                    && endTimestampMillis <= beginTimestampMillis) {
                throw DataXException.asDataXException(DatahubReaderErrorCode.BAD_CONFIG_VALUE,
                        "endTimestampMillis[" + endTimestampMillis + "] must bigger than beginTimestampMillis[" + beginTimestampMillis + "]!");  
            }
        }
        
        @Override
        public void prepare() {
            // create datahub client
            String project = originalConfig.getNecessaryValue(Key.PROJECT, DatahubReaderErrorCode.REQUIRE_VALUE);
            String topic = originalConfig.getNecessaryValue(Key.TOPIC, DatahubReaderErrorCode.REQUIRE_VALUE);
            RecordType recordType = null;
            try {
                DatahubClient client = DatahubClientHelper.getDatahubClient(this.originalConfig);
                GetTopicResult getTopicResult = client.getTopic(project, topic);
                recordType = getTopicResult.getRecordType();
            } catch (Exception e) {
                LOG.warn("get topic type error: {}", e.getMessage());
            }
            if (null != recordType) {
                if (recordType == RecordType.BLOB) {
                    throw DataXException.asDataXException(DatahubReaderErrorCode.BAD_CONFIG_VALUE,
                            "DatahubReader only support 'Tuple' RecordType now, but your RecordType is 'BLOB'");
                }
            }
        }

        @Override
        public void destroy() {
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            LOG.info("split() begin...");
            
            List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();
            
            String project = this.originalConfig.getString(Key.PROJECT);
            String topic = this.originalConfig.getString(Key.TOPIC);
            
            List<ShardEntry> shardEntrys = DatahubReaderUtils.getShardsWithRetry(this.datahubClient, project, topic);
            if (shardEntrys == null || shardEntrys.isEmpty() || shardEntrys.size() == 0) {
                throw DataXException.asDataXException(DatahubReaderErrorCode.BAD_CONFIG_VALUE,
                        "Project [" + project + "] Topic [" + topic + "] has no shards, please check !");       
            }
            
            for (ShardEntry shardEntry : shardEntrys) {
                Configuration splitedConfig = this.originalConfig.clone();
                splitedConfig.set(Key.SHARDID, shardEntry.getShardId());
                readerSplitConfigs.add(splitedConfig);
            }
            
            LOG.info("split() ok and end...");
            return readerSplitConfigs;
        }
        
    }
    
    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        
        private Configuration taskConfig;
        
        private String accessId;
        private String accessKey;
        private String endpoint;
        private String project;
        private String topic;
        private String shardId;
        private Long beginTimestampMillis;
        private Long endTimestampMillis;
        private int batchSize;
        private List<String> columns;
        private RecordSchema schema;
        private String timeStampUnit;
        
        DatahubClient datahubClient;
        
        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();
            
            this.accessId = this.taskConfig.getString(Key.ACCESSKEYID);
            this.accessKey = this.taskConfig.getString(Key.ACCESSKEYSECRET);
            this.endpoint = this.taskConfig.getString(Key.ENDPOINT);
            this.project = this.taskConfig.getString(Key.PROJECT);
            this.topic = this.taskConfig.getString(Key.TOPIC);
            this.shardId = this.taskConfig.getString(Key.SHARDID);
            this.batchSize = this.taskConfig.getInt(Key.BATCHSIZE, 1024);
            this.timeStampUnit = this.taskConfig.getString(Key.TIMESTAMP_UNIT, "MICROSECOND");
            try {
                this.beginTimestampMillis = DatahubReaderUtils.getUnixTimeFromDateTime(this.taskConfig.getString(Key.BEGINDATETIME));
            } catch (ParseException e) {                
            }
            
            try {
                this.endTimestampMillis = DatahubReaderUtils.getUnixTimeFromDateTime(this.taskConfig.getString(Key.ENDDATETIME));
            } catch (ParseException e) {                
            }
            
            this.columns = this.taskConfig.getList(Key.COLUMN, String.class);
            
            this.datahubClient = DatahubClientHelper.getDatahubClient(this.taskConfig);


            this.schema = DatahubReaderUtils.getDatahubSchemaWithRetry(this.datahubClient, this.project, topic);
            
            LOG.info("init datahub reader task finished.project:{} topic:{} batchSize:{}", project, topic, batchSize);
        }

        @Override
        public void destroy() {
        }

        @Override
        public void startRead(RecordSender recordSender) {
            LOG.info("read start");
            
            String beginCursor = DatahubReaderUtils.getCursorWithRetry(this.datahubClient, this.project, 
                    this.topic, this.shardId, this.beginTimestampMillis);
            String endCursor = DatahubReaderUtils.getCursorWithRetry(this.datahubClient, this.project, 
                    this.topic, this.shardId, this.endTimestampMillis);
            
            if (beginCursor == null) {
                LOG.info("Shard:{} has no data!", this.shardId);
                return;
            } else if (endCursor == null) {
                endCursor = DatahubReaderUtils.getLatestCursorWithRetry(this.datahubClient, this.project,
                        this.topic, this.shardId);
            }
            
            String curCursor = beginCursor;
            
            boolean exit = false;
            
            while (true) {
                
                GetRecordsResult result = DatahubReaderUtils.getRecordsResultWithRetry(this.datahubClient, this.project, this.topic,
                        this.shardId, this.batchSize, curCursor, this.schema);
                                
                List<RecordEntry> records = result.getRecords();
                if (records.size() > 0) {
                    for (RecordEntry record : records) {
                        if (record.getSystemTime() >= this.endTimestampMillis) {
                            exit = true;
                            break;
                        }
                        
                        HashMap<String, Column> dataMap = new HashMap<String, Column>();
                        List<Field> fields = ((TupleRecordData) record.getRecordData()).getRecordSchema().getFields();
                        for (int i = 0; i < fields.size(); i++) {
                            Field field = fields.get(i);
                            Column column = DatahubReaderUtils.getColumnFromField(record, field, this.timeStampUnit);
                            dataMap.put(field.getName(), column);
                        }
                        
                        Record dataxRecord = recordSender.createRecord();
                        
                        if (null != this.columns && 1 == this.columns.size()) {
                            String columnsInStr = columns.get(0).toString();
                            if ("\"*\"".equals(columnsInStr) || "*".equals(columnsInStr)) {
                                for (int i = 0; i < fields.size(); i++) {
                                    dataxRecord.addColumn(dataMap.get(fields.get(i).getName()));
                                }

                            } else {
                                if (dataMap.containsKey(columnsInStr)) {
                                    dataxRecord.addColumn(dataMap.get(columnsInStr));
                                } else {
                                    dataxRecord.addColumn(new StringColumn(null));
                                }
                            }
                        } else {
                            for (String col : this.columns) {
                                if (dataMap.containsKey(col)) {
                                    dataxRecord.addColumn(dataMap.get(col));
                                } else {
                                    dataxRecord.addColumn(new StringColumn(null));
                                }
                            }
                        }                         

                        recordSender.sendToWriter(dataxRecord);                           
                    }
                } else {
                    break;
                }
                
                if (exit) {
                    break;
                }
                
                curCursor = result.getNextCursor();
            }
            
            
            LOG.info("end read datahub shard...");
        }
        
    }

}
