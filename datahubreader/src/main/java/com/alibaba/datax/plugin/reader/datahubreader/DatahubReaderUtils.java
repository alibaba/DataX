package com.alibaba.datax.plugin.reader.datahubreader;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.DataXCaseEnvUtil;
import com.alibaba.datax.common.util.RetryUtil;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.model.*;

public class DatahubReaderUtils {

    public static long getUnixTimeFromDateTime(String dateTime) throws ParseException {
        try {
            String format = Constant.DATETIME_FORMAT;
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
            return simpleDateFormat.parse(dateTime).getTime();
        } catch (ParseException ignored) {
            throw DataXException.asDataXException(DatahubReaderErrorCode.BAD_CONFIG_VALUE,
                    "Invalid DateTime[" + dateTime + "]!");   
        }
    }
    
    public static List<ShardEntry> getShardsWithRetry(final DatahubClient datahubClient, final String project, final String topic) {
        
        List<ShardEntry> shards = null;
        try {
            shards = RetryUtil.executeWithRetry(new Callable<List<ShardEntry>>() {
                @Override
                public List<ShardEntry> call() throws Exception {
                    ListShardResult listShardResult = datahubClient.listShard(project, topic);
                    return listShardResult.getShards(); 
                }
            }, DataXCaseEnvUtil.getRetryTimes(7), DataXCaseEnvUtil.getRetryInterval(1000L), DataXCaseEnvUtil.getRetryExponential(true));
            
        } catch (Exception e) {
            throw DataXException.asDataXException(DatahubReaderErrorCode.BAD_CONFIG_VALUE,
                    "get Shards error, please check ! detail error messsage: " + e.toString());
        }         
        return shards;
    }
    
    public static String getCursorWithRetry(final DatahubClient datahubClient, final String project, final String topic, 
            final String shardId, final long timestamp) {
        
        String cursor;
        try {
            cursor = RetryUtil.executeWithRetry(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    try {
                        return datahubClient.getCursor(project, topic, shardId, CursorType.SYSTEM_TIME, timestamp).getCursor();
                    } catch (InvalidParameterException e) {
                        if (e.getErrorMessage().indexOf("Time in seek request is out of range") >= 0) {
                            return null;
                        } else {
                            throw e;
                        }
                        
                    }
                }
            }, DataXCaseEnvUtil.getRetryTimes(7), DataXCaseEnvUtil.getRetryInterval(1000L), DataXCaseEnvUtil.getRetryExponential(true));
            
        } catch (Exception e) {
            throw DataXException.asDataXException(DatahubReaderErrorCode.BAD_CONFIG_VALUE,
                    "get Cursor error, please check ! detail error messsage: " + e.toString());
        }         
        return cursor;
    }
    
    public static String getLatestCursorWithRetry(final DatahubClient datahubClient, final String project, final String topic,
            final String shardId) {
        
        String cursor;
        try {
            cursor = RetryUtil.executeWithRetry(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return datahubClient.getCursor(project, topic, shardId, CursorType.LATEST).getCursor();
                }
            }, DataXCaseEnvUtil.getRetryTimes(7), DataXCaseEnvUtil.getRetryInterval(1000L), DataXCaseEnvUtil.getRetryExponential(true));
            
        } catch (Exception e) {
            throw DataXException.asDataXException(DatahubReaderErrorCode.BAD_CONFIG_VALUE,
                    "get Cursor error, please check ! detail error messsage: " + e.toString());
        }         
        return cursor;
    }    
    
    public static RecordSchema getDatahubSchemaWithRetry(final DatahubClient datahubClient, final String project, final String topic) {
        
        RecordSchema schema;
        try {
            schema = RetryUtil.executeWithRetry(new Callable<RecordSchema>() {
                @Override
                public RecordSchema call() throws Exception {
                    return datahubClient.getTopic(project, topic).getRecordSchema();
                }
            }, DataXCaseEnvUtil.getRetryTimes(7), DataXCaseEnvUtil.getRetryInterval(1000L), DataXCaseEnvUtil.getRetryExponential(true));
            
        } catch (Exception e) {
            throw DataXException.asDataXException(DatahubReaderErrorCode.BAD_CONFIG_VALUE,
                    "get Topic Schema error, please check ! detail error messsage: " + e.toString());
        }         
        return schema;
    } 
    
    public static GetRecordsResult getRecordsResultWithRetry(final DatahubClient datahubClient, final String project,
            final String topic, final String shardId, final int batchSize, final String cursor, final RecordSchema schema) {
        
        GetRecordsResult result;
        try  {
            result = RetryUtil.executeWithRetry(new Callable<GetRecordsResult>() {
                @Override
                public GetRecordsResult call() throws Exception {
                    return datahubClient.getRecords(project, topic, shardId, schema, cursor, batchSize);
                }
            }, DataXCaseEnvUtil.getRetryTimes(7), DataXCaseEnvUtil.getRetryInterval(1000L), DataXCaseEnvUtil.getRetryExponential(true));
            
        } catch (Exception e) {
            throw DataXException.asDataXException(DatahubReaderErrorCode.BAD_CONFIG_VALUE,
                    "get Record Result error, please check ! detail error messsage: " + e.toString());
        }     
        return result;
        
    }
    
    public static Column getColumnFromField(RecordEntry record, Field field, String timeStampUnit) {
        Column col = null;
        TupleRecordData o = (TupleRecordData) record.getRecordData();

        switch (field.getType()) {
            case SMALLINT:
                Short shortValue = ((Short) o.getField(field.getName()));
                col = new LongColumn(shortValue == null ? null: shortValue.longValue());
                break;
            case INTEGER:
                col = new LongColumn((Integer) o.getField(field.getName()));
                break;
            case BIGINT: {
                col = new LongColumn((Long) o.getField(field.getName()));
                break;
            }
            case TINYINT: {
                Byte byteValue = ((Byte) o.getField(field.getName()));
                col = new LongColumn(byteValue == null ? null : byteValue.longValue());
                break;
            }
            case BOOLEAN: {
                col = new BoolColumn((Boolean) o.getField(field.getName()));
                break;
            }
            case FLOAT:
                col = new DoubleColumn((Float) o.getField(field.getName()));
                break;
            case DOUBLE: {
                col = new DoubleColumn((Double) o.getField(field.getName()));
                break;
            }
            case STRING: {
                col = new StringColumn((String) o.getField(field.getName()));
                break;
            }
            case DECIMAL: {
                BigDecimal value = (BigDecimal) o.getField(field.getName());
                col = new DoubleColumn(value == null ? null : value.doubleValue());
                break;
            }
            case TIMESTAMP: {
                Long value = (Long) o.getField(field.getName());

                if ("MILLISECOND".equals(timeStampUnit)) {
                    // MILLISECOND, 13位精度，直接 new Date()
                    col = new DateColumn(value == null ? null : new Date(value));
                }
                else if ("SECOND".equals(timeStampUnit)){
                    col = new DateColumn(value == null ? null : new Date(value * 1000));
                }
                else {
                    // 默认都是 MICROSECOND, 16位精度， 和之前的逻辑保持一致。
                    col = new DateColumn(value == null ? null : new Date(value / 1000));
                }
                break;
            }
            default:
                throw new RuntimeException("Unknown column type: " + field.getType());
        }
        
        return col;
    }
    
}
