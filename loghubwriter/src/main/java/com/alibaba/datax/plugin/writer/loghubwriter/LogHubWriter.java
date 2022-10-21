package com.alibaba.datax.plugin.writer.loghubwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.common.util.StrUtil;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.request.ListShardRequest;
import com.aliyun.openservices.log.request.PutLogsRequest;
import com.aliyun.openservices.log.response.ListShardResponse;
import com.aliyun.openservices.log.response.PutLogsResponse;

import org.apache.commons.codec.digest.Md5Crypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.provider.MD5;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * SLS 写插件
 * @author
 */
public class LogHubWriter extends Writer {

    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration jobConfig = null;

        @Override
        public void init() {
            info(LOG, "loghub writer job init begin ...");
            this.jobConfig = super.getPluginJobConf();
            validateParameter(jobConfig);
            info(LOG, "loghub writer job init end.");
        }

        private void validateParameter(Configuration conf){
            conf.getNecessaryValue(Key.ENDPOINT,LogHubWriterErrorCode.REQUIRE_VALUE);
            conf.getNecessaryValue(Key.ACCESS_KEY_ID,LogHubWriterErrorCode.REQUIRE_VALUE);
            conf.getNecessaryValue(Key.ACCESS_KEY_SECRET,LogHubWriterErrorCode.REQUIRE_VALUE);
            conf.getNecessaryValue(Key.PROJECT,LogHubWriterErrorCode.REQUIRE_VALUE);
            conf.getNecessaryValue(Key.LOG_STORE,LogHubWriterErrorCode.REQUIRE_VALUE);
            conf.getNecessaryValue(Key.COLUMN,LogHubWriterErrorCode.REQUIRE_VALUE);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            info(LOG, "split begin...");
            List<Configuration> configurationList = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                configurationList.add(this.jobConfig.clone());
            }
            info(LOG, "split end...");
            return configurationList;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private Configuration taskConfig;
        private com.aliyun.openservices.log.Client logHubClient;
        private String logStore;
        private String topic;
        private String project;
        private List<String> columnList;
        private int batchSize;
        private String timeCol;
        private String timeFormat;
        private String source;
        private boolean isHashKey;
        private List<Shard> shards;
        public void init() {
            this.taskConfig = super.getPluginJobConf();
            String endpoint = taskConfig.getString(Key.ENDPOINT);
            String accessKeyId = taskConfig.getString(Key.ACCESS_KEY_ID);
            String accessKeySecret = taskConfig.getString(Key.ACCESS_KEY_SECRET);
            project = taskConfig.getString(Key.PROJECT);
            logStore = taskConfig.getString(Key.LOG_STORE);
            topic = taskConfig.getString(Key.TOPIC,"");
            columnList = taskConfig.getList(Key.COLUMN,String.class);
            batchSize = taskConfig.getInt(Key.BATCH_SIZE,1024);
            timeCol = taskConfig.getString(Key.TIME,"");
            timeFormat = taskConfig.getString(Key.TIME_FORMAT,"");
            source = taskConfig.getString(Key.SOURCE,"");
            isHashKey = taskConfig.getBool(Key.HASH_BY_KEY,false);
            logHubClient = new Client(endpoint, accessKeyId, accessKeySecret);
            if (isHashKey) {
                listShard();
                info(LOG, "init loghub writer with hash key mode.");
            }
            if (LOG.isInfoEnabled()) {
                LOG.info("init loghub writer task finished.project:{} logstore:{} topic:{} batchSize:{}",project,logStore,topic,batchSize);
            }
        }

        /**
         * 获取通道的分片信息
         */
        private void listShard() {
            try {
                ListShardResponse response = logHubClient.ListShard(new ListShardRequest(project,logStore));
                shards = response.GetShards();
                if (LOG.isInfoEnabled()) {
                    LOG.info("Get shard count:{}", shards.size());
                }
            } catch (LogException e) {
                info(LOG, "Get shard failed!");
                throw new RuntimeException("Get shard failed!", e);
            }
        }

        @Override
        public void prepare() {
        }

        private int getTime(String v) {
            try {
                if ("bigint".equalsIgnoreCase(timeFormat)) {
                    return Integer.valueOf(v);
                }

                DateFormat sdf = new SimpleDateFormat(timeFormat);
                Date date = sdf.parse(v);
                return (int)(date.getTime()/1000);
            } catch (Exception e) {
                LOG.warn("Format time failed!", e);
            }
            return (int)(((new Date())).getTime()/1000);
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            info(LOG, "start to  write.....................");
            // 按照shared做hash处理
            if (isHashKey) {
                processDataWithHashKey(recordReceiver);
            } else {
                processDataWithoutHashKey(recordReceiver);
            }
            info(LOG, "finish to write.........");
        }

        private void processDataWithHashKey(RecordReceiver receiver) {
            Record record;
            Map<String, List<LogItem>> logMap = new HashMap<String, List<LogItem>>(shards.size());
            int count = 0;
            try {
                while ((record = receiver.getFromReader()) != null) {
                    LogItem logItem = new LogItem();
                    if (record.getColumnNumber() != columnList.size()) {
                        this.getTaskPluginCollector().collectDirtyRecord(record, "column not match");
                    }

                    String id = "";
                    for (int i = 0; i < record.getColumnNumber(); i++) {
                        String colName = columnList.get(i);
                        String colValue = record.getColumn(i).asString();
                        if (colName.endsWith("_id")) {
                            id = colValue;
                        }

                        logItem.PushBack(colName, colValue);
                        if (colName.equals(timeCol)) {
                            logItem.SetTime(getTime(colValue));
                        }
                    }

                    String hashKey = getShardHashKey(StrUtil.getMd5(id), shards);
                    if (!logMap.containsKey(hashKey)) {
                        info(LOG, "Hash key:" + hashKey);
                        logMap.put(hashKey, new ArrayList<LogItem>());
                    }
                    logMap.get(hashKey).add(logItem);

                    if (logMap.get(hashKey).size() % batchSize == 0) {
                        PutLogsRequest request = new PutLogsRequest(project, logStore, topic, source, logMap.get(hashKey), hashKey);
                        PutLogsResponse response = putLog(request);
                        count += logMap.get(hashKey).size();
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("record count:{}, request id:{}", logMap.get(hashKey).size(), response.GetRequestId());
                        }
                        logMap.get(hashKey).clear();
                    }
                }

                for (Map.Entry<String, List<LogItem>> entry : logMap.entrySet()) {
                    if (!entry.getValue().isEmpty()) {
                        // 将剩余的数据发送
                        PutLogsRequest request = new PutLogsRequest(project, logStore, topic, source, entry.getValue(), entry.getKey());
                        PutLogsResponse response = putLog(request);
                        count += entry.getValue().size();
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("record count:{}, request id:{}", entry.getValue().size(), response.GetRequestId());
                        }
                        entry.getValue().clear();
                    }
                }
                LOG.info("{} records have been sent", count);
            } catch (LogException ex) {
                throw DataXException.asDataXException(LogHubWriterErrorCode.LOG_HUB_ERROR, ex.getMessage(), ex);
            } catch (Exception e) {
                throw DataXException.asDataXException(LogHubWriterErrorCode.LOG_HUB_ERROR, e.getMessage(), e);
            }
        }

        private void processDataWithoutHashKey(RecordReceiver receiver) {
            Record record;
            ArrayList<LogItem> logGroup = new ArrayList<LogItem>();
            int count = 0;
            try {
                while ((record = receiver.getFromReader()) != null) {
                    LogItem logItem = new LogItem();
                    if(record.getColumnNumber() != columnList.size()){
                        this.getTaskPluginCollector().collectDirtyRecord(record,"column not match");
                    }
                    for (int i = 0; i < record.getColumnNumber(); i++) {
                        String colName = columnList.get(i);
                        String colValue = record.getColumn(i).asString();
                        logItem.PushBack(colName, colValue);
                        if(colName.equals(timeCol)){
                            logItem.SetTime(getTime(colValue));
                        }
                    }

                    logGroup.add(logItem);
                    count++;
                    if (count % batchSize == 0) {
                        PutLogsRequest request = new PutLogsRequest(project, logStore, topic, source, logGroup);
                        PutLogsResponse response = putLog(request);
                        logGroup.clear();
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("record count:{}, request id:{}", count, response.GetRequestId());
                        }
                    }
                }
                if (!logGroup.isEmpty()) {
                    //将剩余的数据发送
                    PutLogsRequest request = new PutLogsRequest(project, logStore, topic, source, logGroup);
                    PutLogsResponse response = putLog(request);
                    logGroup.clear();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("record count:{}, request id:{}", count, response.GetRequestId());
                    }
                }
                LOG.info("{} records have been sent", count);
            } catch (LogException ex) {
                throw DataXException.asDataXException(LogHubWriterErrorCode.LOG_HUB_ERROR, ex.getMessage(), ex);
            } catch (Exception e) {
                throw DataXException.asDataXException(LogHubWriterErrorCode.LOG_HUB_ERROR, e.getMessage(), e);
            }
        }

        private PutLogsResponse putLog(final PutLogsRequest request) throws Exception{
            final Client client = this.logHubClient;

            return RetryUtil.executeWithRetry(new Callable<PutLogsResponse>() {
                public PutLogsResponse call() throws LogException{
                    return client.PutLogs(request);
                }
            }, 3, 1000L, false);
        }

        private String getShardHashKey(String hashKey, List<Shard> shards) {
            for (Shard shard : shards) {
                if (hashKey.compareTo(shard.getExclusiveEndKey()) < 0 && hashKey.compareTo(shard.getInclusiveBeginKey()) >= 0) {
                    return shard.getInclusiveBeginKey();
                }
            }
            return shards.get(0).getInclusiveBeginKey();
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }

    /**
     * 日志打印控制
     *
     * @param logger
     * @param message
     */
    public static void info(Logger logger, String message) {
        if (logger.isInfoEnabled()) {
            logger.info(message);
        }
    }
}
