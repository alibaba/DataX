package com.alibaba.datax.plugin.reader.loghubreader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.DataXCaseEnvUtil;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.fastjson2.JSONObject;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Consts.CursorMode;
import com.aliyun.openservices.log.common.*;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.BatchGetLogResponse;
import com.aliyun.openservices.log.response.GetCursorResponse;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;

public class LogHubReader extends Reader {
    public static class Job extends Reader.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        
        private Client client;
        private Configuration originalConfig;
        
        private Long beginTimestampMillis;
        private Long endTimestampMillis;
        
        @Override
        public void init() {            
            LOG.info("loghub reader job init begin ...");
            this.originalConfig = super.getPluginJobConf();
            validateParameter(originalConfig);
            
            String endPoint = this.originalConfig.getString(Key.ENDPOINT);
            String accessKeyId = this.originalConfig.getString(Key.ACCESSKEYID);
            String accessKeySecret = this.originalConfig.getString(Key.ACCESSKEYSECRET);
            
            client = new Client(endPoint, accessKeyId, accessKeySecret);
            LOG.info("loghub reader job init end.");
        }
        
        private void validateParameter(Configuration conf){
            conf.getNecessaryValue(Key.ENDPOINT,LogHubReaderErrorCode.REQUIRE_VALUE);
            conf.getNecessaryValue(Key.ACCESSKEYID,LogHubReaderErrorCode.REQUIRE_VALUE);
            conf.getNecessaryValue(Key.ACCESSKEYSECRET,LogHubReaderErrorCode.REQUIRE_VALUE);
            conf.getNecessaryValue(Key.PROJECT,LogHubReaderErrorCode.REQUIRE_VALUE);
            conf.getNecessaryValue(Key.LOGSTORE,LogHubReaderErrorCode.REQUIRE_VALUE);
            conf.getNecessaryValue(Key.COLUMN,LogHubReaderErrorCode.REQUIRE_VALUE);
            
            int batchSize = this.originalConfig.getInt(Key.BATCHSIZE);
            if (batchSize > 1000) {
                throw DataXException.asDataXException(LogHubReaderErrorCode.BAD_CONFIG_VALUE,
                        "Invalid batchSize[" + batchSize + "] value (0,1000]!");
            }
            
            beginTimestampMillis = this.originalConfig.getLong(Key.BEGINTIMESTAMPMILLIS);
            String beginDateTime = this.originalConfig.getString(Key.BEGINDATETIME);
            
            if (beginDateTime != null) {
                try {
                    beginTimestampMillis = getUnixTimeFromDateTime(beginDateTime);
                } catch (ParseException e) {
                    throw DataXException.asDataXException(LogHubReaderErrorCode.BAD_CONFIG_VALUE,
                            "Invalid beginDateTime[" + beginDateTime + "], format [yyyyMMddHHmmss or yyyyMMdd]!");    
                }
            }
            
            if (beginTimestampMillis != null && beginTimestampMillis <= 0) {
                throw DataXException.asDataXException(LogHubReaderErrorCode.BAD_CONFIG_VALUE,
                        "Invalid beginTimestampMillis[" + beginTimestampMillis + "]!");               
            }
            
            endTimestampMillis = this.originalConfig.getLong(Key.ENDTIMESTAMPMILLIS); 
            String endDateTime = this.originalConfig.getString(Key.ENDDATETIME);
            
            if (endDateTime != null) {
                try {
                    endTimestampMillis = getUnixTimeFromDateTime(endDateTime);
                } catch (ParseException e) {
                    throw DataXException.asDataXException(LogHubReaderErrorCode.BAD_CONFIG_VALUE,
                            "Invalid beginDateTime[" + endDateTime + "], format [yyyyMMddHHmmss or yyyyMMdd]!");    
                }
            }
            
            if (endTimestampMillis != null && endTimestampMillis <= 0) {
                throw DataXException.asDataXException(LogHubReaderErrorCode.BAD_CONFIG_VALUE,
                        "Invalid endTimestampMillis[" + endTimestampMillis + "]!");                
            }
            
            if (beginTimestampMillis != null && endTimestampMillis != null
                    && endTimestampMillis <= beginTimestampMillis) {
                throw DataXException.asDataXException(LogHubReaderErrorCode.BAD_CONFIG_VALUE,
                        "endTimestampMillis[" + endTimestampMillis + "] must bigger than beginTimestampMillis[" + beginTimestampMillis + "]!");  
            }
        }
        
        private long getUnixTimeFromDateTime(String dateTime) throws ParseException {
            try {
                String format = Constant.DATETIME_FORMAT;
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
                return simpleDateFormat.parse(dateTime).getTime() / 1000;
            } catch (ParseException ignored) {
                throw DataXException.asDataXException(LogHubReaderErrorCode.BAD_CONFIG_VALUE,
                        "Invalid DateTime[" + dateTime + "]!");   
            }
        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            LOG.info("split() begin...");
            
            List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();
            
            final String project = this.originalConfig.getString(Key.PROJECT);
            final String logstore = this.originalConfig.getString(Key.LOGSTORE);
            
            List<Shard> logStore = null;
            try {
                logStore = RetryUtil.executeWithRetry(new Callable<List<Shard>>() {
                    @Override
                    public List<Shard> call() throws Exception {
                        return client.ListShard(project, logstore).GetShards();
                    }
                }, DataXCaseEnvUtil.getRetryTimes(7), DataXCaseEnvUtil.getRetryInterval(1000L), DataXCaseEnvUtil.getRetryExponential(true));
            } catch (Exception e) {
                throw DataXException.asDataXException(LogHubReaderErrorCode.BAD_CONFIG_VALUE,
                        "get LogStore[" + logstore + "] error, please check ! detail error messsage: " + e.toString());
            } 
            
            if (logStore == null) {
                throw DataXException.asDataXException(LogHubReaderErrorCode.BAD_CONFIG_VALUE,
                        "LogStore[" + logstore + "] isn't exists, please check !");                
            }
            
            int splitNumber = logStore.size();
            if (0 == splitNumber) {
                throw DataXException.asDataXException(LogHubReaderErrorCode.EMPTY_LOGSTORE_VALUE,
                        "LogStore[" + logstore + "] has 0 shard, please check !");
            }
            
            Collections.shuffle(logStore);
            for (int i = 0; i < logStore.size(); i++) {
                if (beginTimestampMillis != null && endTimestampMillis != null) {
                    try {
                        String beginCursor = getCursorWithRetry(client, project, logstore, logStore.get(i).GetShardId(), beginTimestampMillis).GetCursor();
                        String endCursor = getCursorWithRetry(client, project, logstore, logStore.get(i).GetShardId(), endTimestampMillis).GetCursor();
                        if (beginCursor.equals(endCursor)) {
                            if ((i == logStore.size() - 1) && (readerSplitConfigs.size() == 0)) {
                                
                            } else {
                                LOG.info("skip empty shard[" + logStore.get(i) + "]!");
                                continue;
                            }
                        }
                    } catch (Exception e) {
                        LOG.error("Check Shard[" + logStore.get(i) + "] Error, please check !" + e.toString());
                        throw DataXException.asDataXException(LogHubReaderErrorCode.LOG_HUB_ERROR, e);
                    }
                } 
                Configuration splitedConfig = this.originalConfig.clone();
                splitedConfig.set(Key.SHARD, logStore.get(i).GetShardId());
                readerSplitConfigs.add(splitedConfig);
            }
            
            if (splitNumber < adviceNumber) {
        //        LOG.info(MESSAGE_SOURCE.message("hdfsreader.12",
        //                splitNumber, adviceNumber, splitNumber, splitNumber));
            }
            LOG.info("split() ok and end...");
            
            return readerSplitConfigs;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
        
        private GetCursorResponse getCursorWithRetry(final Client client, final String project, final String logstore, final int shard, final long fromTime) throws Exception {
            return 
                    RetryUtil.executeWithRetry(new Callable<GetCursorResponse>() {
                @Override
                public GetCursorResponse call() throws Exception {
                    LOG.info("loghug get cursor with project: {} logstore: {} shard: {} time: {}", project, logstore, shard, fromTime);
                    return client.GetCursor(project, logstore, shard, fromTime);
                }
            }, 7, 1000L, true);
        }

    }

    public static class Task extends Reader.Task {
        
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        
        private Configuration taskConfig;
        private Client client;
        private String endPoint;
        private String accessKeyId;
        private String accessKeySecret;
        private String project;
        private String logstore;
        private long beginTimestampMillis;
        private long endTimestampMillis;
        private int batchSize;
        private int shard;
        private List<String> columns;
                
        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();
            
            endPoint = this.taskConfig.getString(Key.ENDPOINT);
            accessKeyId = this.taskConfig.getString(Key.ACCESSKEYID);
            accessKeySecret = this.taskConfig.getString(Key.ACCESSKEYSECRET);
            project = this.taskConfig.getString(Key.PROJECT);
            logstore = this.taskConfig.getString(Key.LOGSTORE);
            batchSize = this.taskConfig.getInt(Key.BATCHSIZE, 128);
            
            this.beginTimestampMillis = this.taskConfig.getLong(Key.BEGINTIMESTAMPMILLIS, -1);            
            String beginDateTime = this.taskConfig.getString(Key.BEGINDATETIME);
            
            if (beginDateTime != null) {
                try {
                    beginTimestampMillis = getUnixTimeFromDateTime(beginDateTime);
                } catch (ParseException e) {                    
                }
            }
                        
            this.endTimestampMillis = this.taskConfig.getLong(Key.ENDTIMESTAMPMILLIS, -1);     
            String endDateTime = this.taskConfig.getString(Key.ENDDATETIME);
            
            if (endDateTime != null) {
                try {
                    endTimestampMillis = getUnixTimeFromDateTime(endDateTime);
                } catch (ParseException e) {                    
                }
            }
            
            columns = this.taskConfig.getList(Key.COLUMN, String.class);
            
            shard = this.taskConfig.getInt(Key.SHARD);
            
            client = new Client(endPoint, accessKeyId, accessKeySecret);
            LOG.info("init loghub reader task finished.project:{} logstore:{} batchSize:{}", project, logstore, batchSize);
        }

        @Override
        public void prepare() {
        }
        
        private long getUnixTimeFromDateTime(String dateTime) throws ParseException {
            try {
                String format = Constant.DATETIME_FORMAT;
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
                return simpleDateFormat.parse(dateTime).getTime() / 1000;
            } catch (ParseException ignored) {
            }
            String format = Constant.DATE_FORMAT;
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
            return simpleDateFormat.parse(dateTime).getTime() / 1000;
        }
        
        private GetCursorResponse getCursorWithRetry(final Client client, final String project, final String logstore, final int shard, final long fromTime) throws Exception {
            return 
                    RetryUtil.executeWithRetry(new Callable<GetCursorResponse>() {
                @Override
                public GetCursorResponse call() throws Exception {
                    LOG.info("loghug get cursor with project: {} logstore: {} shard: {} time: {}", project, logstore, shard, fromTime);
                    return client.GetCursor(project, logstore, shard, fromTime);
                }
            }, 7, 1000L, true);
        }
        
        private GetCursorResponse getCursorWithRetry(final Client client, final String project, final String logstore, final int shard, final CursorMode mode) throws Exception {
            return 
                    RetryUtil.executeWithRetry(new Callable<GetCursorResponse>() {
                @Override
                public GetCursorResponse call() throws Exception {
                    LOG.info("loghug get cursor with project: {} logstore: {} shard: {} mode: {}", project, logstore, shard, mode);
                    return client.GetCursor(project, logstore, shard, mode);
                }
            }, 7, 1000L, true);
        }
        
        private BatchGetLogResponse batchGetLogWithRetry(final Client client, final String project, final String logstore, final int shard, final int batchSize,
                final String curCursor, final String endCursor) throws Exception {
            return 
                    RetryUtil.executeWithRetry(new Callable<BatchGetLogResponse>() {
                @Override
                public BatchGetLogResponse call() throws Exception {
                    return client.BatchGetLog(project, logstore, shard, batchSize, curCursor, endCursor);
                }
            }, 7, 1000L, true);
        }

        @Override
        public void startRead(RecordSender recordSender) {
            LOG.info("read start");
            
            try {                                
                GetCursorResponse cursorRes;
                if (this.beginTimestampMillis != -1) {
                    cursorRes = getCursorWithRetry(client, project, logstore, this.shard, beginTimestampMillis);                     
                } else {
                    cursorRes = getCursorWithRetry(client, project, logstore, this.shard, CursorMode.BEGIN);
                }
                String beginCursor = cursorRes.GetCursor();
                
                LOG.info("the begin cursor, loghub requestId: {} cursor: {}", cursorRes.GetRequestId(), cursorRes.GetCursor());
                                
                if (this.endTimestampMillis != -1) {
                    cursorRes = getCursorWithRetry(client, project, logstore, this.shard, endTimestampMillis); 
                } else {
                    cursorRes = getCursorWithRetry(client, project, logstore, this.shard, CursorMode.END);
                }
                String endCursor = cursorRes.GetCursor();
                LOG.info("the end cursor, loghub requestId: {} cursor: {}", cursorRes.GetRequestId(), cursorRes.GetCursor());

                if (StringUtils.equals(beginCursor, endCursor)) {
                    LOG.info("beginCursor:{} equals endCursor:{}, end directly!", beginCursor, endCursor);
                    return;
                }

                String currentCursor = null;
                String nextCursor = beginCursor;

                HashMap<String, String> metaMap = new HashMap<String, String>();
                HashMap<String, String> dataMap = new HashMap<String, String>();
                JSONObject allMetaJson = new JSONObject();
                while (!StringUtils.equals(currentCursor, nextCursor)) {
                    currentCursor = nextCursor;
                    BatchGetLogResponse logDataRes = batchGetLogWithRetry(client, project, logstore, this.shard, this.batchSize, currentCursor, endCursor);
                                        
                    List<LogGroupData> logGroups = logDataRes.GetLogGroups();
                    
                    for(LogGroupData logGroup: logGroups) {
                        metaMap.clear();
                        allMetaJson.clear();
                        FastLogGroup flg = logGroup.GetFastLogGroup();

                        metaMap.put("C_Category", flg.getCategory());
                        metaMap.put(Constant.META_COL_CATEGORY, flg.getCategory());
                        allMetaJson.put(Constant.META_COL_CATEGORY, flg.getCategory());

                        metaMap.put("C_Source", flg.getSource());
                        metaMap.put(Constant.META_COL_SOURCE, flg.getSource());
                        allMetaJson.put(Constant.META_COL_SOURCE, flg.getSource());

                        metaMap.put("C_Topic", flg.getTopic());
                        metaMap.put(Constant.META_COL_TOPIC, flg.getTopic());
                        allMetaJson.put(Constant.META_COL_TOPIC, flg.getTopic());

                        metaMap.put("C_MachineUUID", flg.getMachineUUID());
                        metaMap.put(Constant.META_COL_MACHINEUUID, flg.getMachineUUID());
                        allMetaJson.put(Constant.META_COL_MACHINEUUID, flg.getMachineUUID());
                                                                    
                        for (int tagIdx = 0; tagIdx < flg.getLogTagsCount(); ++tagIdx) {
                            FastLogTag logtag = flg.getLogTags(tagIdx);
                            String tagKey = logtag.getKey();
                            String tagValue = logtag.getValue();
                            if (tagKey.equals(Constant.META_COL_HOSTNAME)) {
                                metaMap.put("C_HostName", logtag.getValue());
                            } else if (tagKey.equals(Constant.META_COL_PATH)) {
                                metaMap.put("C_Path", logtag.getValue());
                            }
                            metaMap.put(tagKey, tagValue);
                            allMetaJson.put(tagKey, tagValue);
                        }

                        for (int lIdx = 0; lIdx < flg.getLogsCount(); ++lIdx) {
                            dataMap.clear();
                            FastLog log = flg.getLogs(lIdx);
                            
                            String logTime = String.valueOf(log.getTime());
                            metaMap.put("C_LogTime", logTime);
                            metaMap.put(Constant.META_COL_LOGTIME, logTime);
                            allMetaJson.put(Constant.META_COL_LOGTIME, logTime);

                            for (int cIdx = 0; cIdx < log.getContentsCount(); ++cIdx) {
                                FastLogContent content = log.getContents(cIdx);
                                dataMap.put(content.getKey(), content.getValue());  
                            }
                            
                            Record record = recordSender.createRecord();

                            JSONObject extractOthers = new JSONObject();
                            if(columns.contains(Constant.COL_EXTRACT_OTHERS)){
                                List<String> keyList = Arrays.asList(dataMap.keySet().toArray(new String[dataMap.keySet().size()]));
                                for (String otherKey:keyList) {
                                    if (!columns.contains(otherKey)){
                                        extractOthers.put(otherKey,dataMap.get(otherKey));
                                    }
                                }
                            }
                            if (null != this.columns && 1 == this.columns.size()) {
                                String columnsInStr = columns.get(0).toString();
                                if ("\"*\"".equals(columnsInStr) || "*".equals(columnsInStr)) {
                                    List<String> keyList = Arrays.asList(dataMap.keySet().toArray(new String[dataMap.keySet().size()]));
                                    Collections.sort(keyList);

                                    for (String key : keyList) {
                                        record.addColumn(new StringColumn(key + ":" + dataMap.get(key)));
                                    }
                                } else {
                                    if (dataMap.containsKey(columnsInStr)) {
                                        record.addColumn(new StringColumn(dataMap.get(columnsInStr)));
                                    } else if (metaMap.containsKey(columnsInStr)) {
                                        record.addColumn(new StringColumn(metaMap.get(columnsInStr)));
                                    } else if (Constant.COL_EXTRACT_OTHERS.equals(columnsInStr)){
                                        record.addColumn(new StringColumn(extractOthers.toJSONString()));
                                    } else if (Constant.COL_EXTRACT_ALL_META.equals(columnsInStr)) {
                                        record.addColumn(new StringColumn(allMetaJson.toJSONString()));
                                    }
                                }
                            } else {
                                for (String col : this.columns) {
                                    if (dataMap.containsKey(col)) {
                                        record.addColumn(new StringColumn(dataMap.get(col)));
                                    } else if (metaMap.containsKey(col)) {
                                        record.addColumn(new StringColumn(metaMap.get(col)));
                                    } else if (col != null && col.startsWith("'") && col.endsWith("'")){
                                        String constant = col.substring(1, col.length()-1);
                                        record.addColumn(new StringColumn(constant));
                                    }else if (Constant.COL_EXTRACT_OTHERS.equals(col)){
                                        record.addColumn(new StringColumn(extractOthers.toJSONString()));
                                    } else if (Constant.COL_EXTRACT_ALL_META.equals(col)) {
                                        record.addColumn(new StringColumn(allMetaJson.toJSONString()));
                                    } else {
                                        record.addColumn(new StringColumn(null));
                                    }
                                }
                            }                         

                            recordSender.sendToWriter(record);                            
                        }
                    }

                    nextCursor = logDataRes.GetNextCursor();
                }
            } catch (LogException e) {
                if (e.GetErrorCode().equals("LogStoreNotExist")) {
                    LOG.info("logStore[" + logstore +"] Not Exits! detail error messsage: " + e.toString());
                } else {
                    LOG.error("read LogStore[" + logstore + "] error, please check ! detail error messsage: " + e.toString());
                    throw DataXException.asDataXException(LogHubReaderErrorCode.LOG_HUB_ERROR, e);             
                }

            } catch (Exception e) {
                LOG.error("read LogStore[" + logstore + "] error, please check ! detail error messsage: " + e.toString());
                throw DataXException.asDataXException(LogHubReaderErrorCode.LOG_HUB_ERROR, e);
            }
                        
            LOG.info("end read loghub shard...");
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }
}
