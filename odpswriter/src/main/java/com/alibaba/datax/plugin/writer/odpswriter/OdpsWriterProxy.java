package com.alibaba.datax.plugin.writer.odpswriter;

import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.MessageSource;
import com.alibaba.datax.plugin.writer.odpswriter.util.OdpsUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.IntervalDayTime;
import com.aliyun.odps.data.IntervalYearMonth;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.ProtobufRecordPack;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.CharTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.VarcharTypeInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;

public class OdpsWriterProxy {
    private static final Logger LOG = LoggerFactory.getLogger(OdpsWriterProxy.class);
    private static final MessageSource MESSAGE_SOURCE = MessageSource.loadResourceBundle(OdpsWriterProxy.class);

    private volatile boolean printColumnLess;// 是否打印对于源头字段数小于 ODPS 目的表的行的日志

    private TaskPluginCollector taskPluginCollector;

    private TableTunnel.UploadSession slaveUpload;
    private TableSchema schema;
    private int maxBufferSize;
    private ProtobufRecordPack protobufRecordPack;
    private int protobufCapacity;
    private AtomicLong blockId;

    private List<Integer> columnPositions;
    private List<TypeInfo> tableOriginalColumnTypeList;
    private boolean emptyAsNull;
    private boolean isCompress;
    
    private int taskId;
    private int taskCOUNT;
    private boolean consistencyCommit = false;
    private boolean checkWithGetSize = true;
    private List<String> allColumns;
    private String overLengthRule;
    private int maxFieldLength;
    private Boolean enableOverLengthOutput;

    /**
     * 记录最近一次活动时间，动态分区写入模式下，超过一定时间不活动，则关闭这个proxy
     */
    private Long lastActiveTime;

    /**
     * 写block超时时间
     */
    private Long writeTimeoutInMs;

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    // 读取 jvm 默认时区
    private Calendar calendarForDate = null;
    private boolean useDateWithCalendar = true;

    private Calendar initCalendar(Configuration config) {
        // 理论上不会有其他选择，有配置化可以随时应急
        String calendarType = config.getString("calendarType", "iso8601");
        Boolean lenient = config.getBool("calendarLenient", true);

        // 默认jvm时区
        TimeZone timeZone = TimeZone.getDefault();
        String timeZoneStr = config.getString("calendarTimeZone");
        if (StringUtils.isNotBlank(timeZoneStr)) {
            // 如果用户明确指定使用用户指定的
            timeZone = TimeZone.getTimeZone(timeZoneStr);
        }

        Calendar calendarForDate = new Calendar.Builder().setCalendarType(calendarType).setLenient(lenient)
                .setTimeZone(timeZone).build();
        return calendarForDate;
    }
    
    public OdpsWriterProxy(TableTunnel.UploadSession slaveUpload, int blockSizeInMB, AtomicLong blockId,
                           List<Integer> columnPositions, TaskPluginCollector taskPluginCollector, boolean emptyAsNull,
                           boolean isCompress, boolean checkWithGetSize, List<String> allColumns, boolean initBufSizeZero,
                           Long writeTimeoutInMs, Configuration taskConfig, String overLengthRule, int maxFieldLength,
                           Boolean enableOverLengthOutput) throws IOException, TunnelException {
        this.slaveUpload = slaveUpload;
        this.schema = this.slaveUpload.getSchema();
        this.tableOriginalColumnTypeList = OdpsUtil.getTableOriginalColumnTypeList(this.schema);

        this.blockId = blockId;
        this.columnPositions = columnPositions;
        this.taskPluginCollector = taskPluginCollector;
        this.emptyAsNull = emptyAsNull;
        this.isCompress = isCompress;

        // 初始化与 buffer 区相关的值
        this.maxBufferSize = (blockSizeInMB - 4) * 1024 * 1024;
        if (initBufSizeZero) {
            // 动态分区下初始化为0，随着写入的reord变多慢慢增加
            this.protobufCapacity = 0;
        } else {
            this.protobufCapacity = blockSizeInMB * 1024 * 1024;
        }
        this.protobufRecordPack = new ProtobufRecordPack(this.schema, null, this.protobufCapacity);
        this.printColumnLess = true;
        this.checkWithGetSize = checkWithGetSize;

        this.allColumns = allColumns;
        this.overLengthRule = overLengthRule;
        this.maxFieldLength = maxFieldLength;
        this.enableOverLengthOutput = enableOverLengthOutput;

        this.writeTimeoutInMs = writeTimeoutInMs;
        
        this.calendarForDate = this.initCalendar(taskConfig);
        this.useDateWithCalendar = taskConfig.getBool("useDateWithCalendar", true);
    }

    public OdpsWriterProxy(TableTunnel.UploadSession slaveUpload, int blockSizeInMB, AtomicLong blockId, int taskId,
                           int taskCount, List<Integer> columnPositions, TaskPluginCollector taskPluginCollector, boolean emptyAsNull,
                           boolean isCompress, boolean checkWithGetSize, List<String> allColumns, Long writeTimeoutInMs, Configuration taskConfig,
                           String overLengthRule, int maxFieldLength, Boolean enableOverLengthOutput) throws IOException, TunnelException {
        this.slaveUpload = slaveUpload;
        this.schema = this.slaveUpload.getSchema();
        this.tableOriginalColumnTypeList = OdpsUtil.getTableOriginalColumnTypeList(this.schema);

        this.blockId = blockId;
        this.columnPositions = columnPositions;
        this.taskPluginCollector = taskPluginCollector;
        this.emptyAsNull = emptyAsNull;
        this.isCompress = isCompress;

        // 初始化与 buffer 区相关的值
        this.maxBufferSize = (blockSizeInMB - 4) * 1024 * 1024;
        this.protobufCapacity = blockSizeInMB * 1024 * 1024;
        this.protobufRecordPack = new ProtobufRecordPack(this.schema, null, this.protobufCapacity);
        printColumnLess = true;
        
        this.taskId = taskId;
        this.taskCOUNT = taskCount;
        this.consistencyCommit = true;
        this.checkWithGetSize = checkWithGetSize;
        this.allColumns = allColumns;
        this.overLengthRule = overLengthRule;
        this.maxFieldLength = maxFieldLength;
        this.enableOverLengthOutput = enableOverLengthOutput;

        this.writeTimeoutInMs = writeTimeoutInMs;
        
        this.calendarForDate = this.initCalendar(taskConfig);
        this.useDateWithCalendar = taskConfig.getBool("useDateWithCalendar", true);
    }

    public long getCurrentBlockId() {
        if (this.consistencyCommit) {
            return this.taskId + this.taskCOUNT * (this.blockId.get());
        } else {
            return this.blockId.get();
        }
    }

    public TableTunnel.UploadSession getSlaveUpload() {
        return this.slaveUpload;
    }
    
    public long writeOneRecord(com.alibaba.datax.common.element.Record dataXRecord, List<Long> blocks)
            throws Exception {

        this.lastActiveTime = System.currentTimeMillis();

        Record record = dataxRecordToOdpsRecord(dataXRecord);

        if (null == record) {
            return 0;
        }
        protobufRecordPack.append(record);

        if (protobufRecordPack.getProtobufStream().size() >= maxBufferSize) {
            long startTimeInNs = System.nanoTime();
            OdpsUtil.slaveWriteOneBlock(this.slaveUpload, protobufRecordPack, getCurrentBlockId(), this.writeTimeoutInMs);
            LOG.info("write block {} ok.", getCurrentBlockId());
            blocks.add(getCurrentBlockId());
            protobufRecordPack.reset();
            this.blockId.incrementAndGet();
            return System.nanoTime() - startTimeInNs;
        }
        return 0;
    }

    public long writeRemainingRecord(List<Long> blocks) throws Exception {
        // complete protobuf stream, then write to http
        // protobufRecordPack.getTotalBytes() 慕明: getTotalBytes并不一定保证能拿到写入的字节数，按你们的逻辑应该是用getTotalBytesWritten
        // if (protobufRecordPack.getTotalBytes() != 0) {
        boolean hasRemindData = false;
        if (this.checkWithGetSize) {
            hasRemindData = protobufRecordPack.getSize() != 0;
        } else {
            hasRemindData = protobufRecordPack.getTotalBytes() != 0;
        }
        if (hasRemindData) {
            long startTimeInNs = System.nanoTime();
            OdpsUtil.slaveWriteOneBlock(this.slaveUpload, protobufRecordPack, getCurrentBlockId(), this.writeTimeoutInMs);
            LOG.info("write block {} ok.", getCurrentBlockId());

            blocks.add(getCurrentBlockId());
            // reset the buffer for next block
            protobufRecordPack.reset();
            return System.nanoTime() - startTimeInNs;
        }
        return 0;
    }

    public Record dataxRecordToOdpsRecord(com.alibaba.datax.common.element.Record dataXRecord) throws Exception {
        int sourceColumnCount = dataXRecord.getColumnNumber();
        ArrayRecord odpsRecord = (ArrayRecord) slaveUpload.newRecord();

        int userConfiguredColumnNumber = this.columnPositions.size();

        if (sourceColumnCount > userConfiguredColumnNumber) {
            throw DataXException.asDataXException(OdpsWriterErrorCode.ILLEGAL_VALUE,
                    MESSAGE_SOURCE.message("odpswriterproxy.1", sourceColumnCount, userConfiguredColumnNumber));
        } else if (sourceColumnCount < userConfiguredColumnNumber) {
            if (printColumnLess) {
                LOG.warn(MESSAGE_SOURCE.message("odpswriterproxy.2", sourceColumnCount, userConfiguredColumnNumber));
            }
            printColumnLess = false;
        }

        int currentIndex = 0;
        int sourceIndex = 0;
        try {
            com.alibaba.datax.common.element.Column columnValue;

            for (; sourceIndex < sourceColumnCount; sourceIndex++) {
                // 跳过分区列
                if (this.columnPositions.get(sourceIndex) == -1) {
                    continue;
                }
                currentIndex = columnPositions.get(sourceIndex);
                TypeInfo typeInfo = this.tableOriginalColumnTypeList.get(currentIndex);
                OdpsType type = typeInfo.getOdpsType();
                String typeName = typeInfo.getTypeName();
                columnValue = dataXRecord.getColumn(sourceIndex);

                if (columnValue == null) {
                    continue;
                }
                // for compatible dt lib, "" as null
                if (this.emptyAsNull && columnValue instanceof StringColumn && "".equals(columnValue.asString())) {
                    continue;
                }

                switch (type) {
                case STRING:
                    String newValue =  (String)OdpsUtil.processOverLengthData(columnValue.asString(), OdpsType.STRING, this.overLengthRule, this.maxFieldLength, this.enableOverLengthOutput);
                    odpsRecord.setString(currentIndex, newValue);
                    break;
                case BIGINT:
                    odpsRecord.setBigint(currentIndex, columnValue.asLong());
                    break;
                case BOOLEAN:
                    odpsRecord.setBoolean(currentIndex, columnValue.asBoolean());
                    break;
                case DATETIME:
                    odpsRecord.setDatetime(currentIndex, columnValue.asDate());
//                    Date datetimeData = columnValue.asDate();
//                    if (null == datetimeData) {
//                        odpsRecord.setDatetime(currentIndex, null);
//                    } else {
//                        Timestamp dateDataForOdps = new Timestamp(datetimeData.getTime());
//                        if (datetimeData instanceof java.sql.Timestamp) {
//                            dateDataForOdps.setNanos(((java.sql.Timestamp)datetimeData).getNanos());
//                        }
//                        odpsRecord.setDatetime(currentIndex, dateDataForOdps);
//                    }
                    break;
                case DATE:
                    Date dateData = columnValue.asDate();
                    if (null == dateData) {
                        odpsRecord.setDatetime(currentIndex, null);
                    } else {
                        if (this.useDateWithCalendar) {
                            odpsRecord.setDate(currentIndex, new java.sql.Date(dateData.getTime()), this.calendarForDate);
                        } else {
                            odpsRecord.setDatetime(currentIndex, new java.sql.Date(dateData.getTime()));
                        }
                    }
                    break;
                case DOUBLE:
                    odpsRecord.setDouble(currentIndex, columnValue.asDouble());
                    break;
                case FLOAT:
                    Double floatValue = columnValue.asDouble();
                    if (null == floatValue) {
                        ((ArrayRecord) odpsRecord).setFloat(currentIndex, null);
                    } else {
                        ((ArrayRecord) odpsRecord).setFloat(currentIndex, floatValue.floatValue());
                    }
                    break;
                case DECIMAL:
                    odpsRecord.setDecimal(currentIndex, columnValue.asBigDecimal());
                    String columnStr = columnValue.asString();
                    if (columnStr != null && columnStr.indexOf(".") >= 36) {
                        throw new Exception(MESSAGE_SOURCE.message("odpswriterproxy.3"));
                    }
                    break;
                case TINYINT:
                    Long tinyintValueStr = columnValue.asLong();
                    if (null == tinyintValueStr) {
                        ((ArrayRecord) odpsRecord).setTinyint(currentIndex, null);
                    } else {
                        ((ArrayRecord) odpsRecord).setTinyint(currentIndex,
                                Byte.valueOf(String.valueOf(tinyintValueStr)));
                    }
                    break;
                case SMALLINT:
                    Long smallIntValue = columnValue.asLong();
                    if (null == smallIntValue) {
                        ((ArrayRecord) odpsRecord).setSmallint(currentIndex, null);
                    } else {
                        ((ArrayRecord) odpsRecord).setSmallint(currentIndex, smallIntValue.shortValue());
                    }
                    break;
                case INT:
                    Long intValue = columnValue.asLong();
                    if (null == intValue) {
                        ((ArrayRecord) odpsRecord).setInt(currentIndex, null);
                    } else {
                        ((ArrayRecord) odpsRecord).setInt(currentIndex, intValue.intValue());
                    }
                    break;
                case VARCHAR:
                    // warn: columnValue.asString() 为 null 时 ， odps sdk 有 BUG
                    // 不能用 Varchar 的默认构造函数，不然有 NPE
                    String varcharValueStr = columnValue.asString();
                    Varchar varcharData = null;
                    if (varcharValueStr != null){
                        varcharData = new Varchar(columnValue.asString());
                    }
                    ((ArrayRecord) odpsRecord).setVarchar(currentIndex, varcharData);
                    break;
                case CHAR:
                    String charValueStr = columnValue.asString();
                    Char charData = null;
                    if (charValueStr != null ){
                        charData = new Char(charValueStr);
                    }
                    ((ArrayRecord) odpsRecord).setChar(currentIndex, charData);
                    break;
                case TIMESTAMP:
                    Date timestampData = columnValue.asDate();
                    if (null == timestampData) {
                        ((ArrayRecord) odpsRecord).setTimestamp(currentIndex, null);
                    } else {
                        Timestamp timestampDataForOdps = new Timestamp(timestampData.getTime());
                        if (timestampData instanceof java.sql.Timestamp) {
                            // 纳秒
                            timestampDataForOdps.setNanos(((java.sql.Timestamp)timestampData).getNanos());
                        }
                        // warn优化：如果原来类型就是Timestamp，直接使用就少创建了一个对象
                        ((ArrayRecord) odpsRecord).setTimestamp(currentIndex, timestampDataForOdps);
                    }
                    break;
                case BINARY:
                    Binary newBinaryData  =  (Binary)OdpsUtil.processOverLengthData(new Binary(columnValue.asBytes()), OdpsType.BINARY, this.overLengthRule, this.maxFieldLength, this.enableOverLengthOutput);
                    ((ArrayRecord) odpsRecord).setBinary(currentIndex,columnValue.asBytes() == null ? null : newBinaryData);
                    break;
                case ARRAY:
                    JSONArray arrayJson = JSON.parseArray(columnValue.asString());
                    ((ArrayRecord) odpsRecord).setArray(currentIndex, parseArray(arrayJson, (ArrayTypeInfo) typeInfo));
                    break;
                case MAP:
                    JSONObject mapJson = JSON.parseObject(columnValue.asString());
                    ((ArrayRecord) odpsRecord).setMap(currentIndex, parseMap(mapJson, (MapTypeInfo) typeInfo));
                    break;
                case STRUCT:
                    JSONObject structJson = JSON.parseObject(columnValue.asString());
                    ((ArrayRecord) odpsRecord).setStruct(currentIndex,
                            parseStruct(structJson, (StructTypeInfo) typeInfo));
                    break;
                default:
                    break;
                }
            }

            return odpsRecord;
        } catch (Exception e) {
            String dirtyColumnName = "";
            try {
                dirtyColumnName = this.allColumns.get(currentIndex);
            } catch (Exception ignoreEx) {
                // ignore
            }
            String message = MESSAGE_SOURCE.message("odpswriterproxy.4", sourceIndex, dirtyColumnName);
            this.taskPluginCollector.collectDirtyRecord(dataXRecord, e, message);
            return null;
        }

    }

    private List parseArray(JSONArray jsonArray, ArrayTypeInfo arrayTypeInfo) throws ParseException {
        if (null == jsonArray) {
            return null;
        }
        List result = new ArrayList();
        switch (arrayTypeInfo.getElementTypeInfo().getOdpsType()) {
        case BIGINT:
            for (int i = 0; i < jsonArray.size(); i++) {
                result.add(jsonArray.getLong(i));
            }
            return result;
        /**
         * 双精度浮点
         */
        case DOUBLE:
            for (int i = 0; i < jsonArray.size(); i++) {
                result.add(jsonArray.getDouble(i));
            }
            return result;
        /**
         * 布尔型
         */
        case BOOLEAN:
            for (int i = 0; i < jsonArray.size(); i++) {
                result.add(jsonArray.getBoolean(i));
            }
            return result;
        /**
         * 日期类型
         */
        case DATETIME:
            // TODO 精度
            for (int i = 0; i < jsonArray.size(); i++) {
                result.add(dateFormat.parse(jsonArray.getString(i)));
            }
            return result;
        /**
         * 字符串类型
         */
        case STRING:
            for (int i = 0; i < jsonArray.size(); i++) {
                result.add(jsonArray.getString(i));
            }
            return result;
        /**
         * 精确小数类型
         */
        case DECIMAL:
            for (int i = 0; i < jsonArray.size(); i++) {
                result.add(jsonArray.getBigDecimal(i));
            }
            return result;
        /**
         * 1字节有符号整型
         */
        case TINYINT:
            for (int i = 0; i < jsonArray.size(); i++) {
                result.add(jsonArray.getByte(i));
            }
            return result;
        /**
         * 2字节有符号整型
         */
        case SMALLINT:
            for (int i = 0; i < jsonArray.size(); i++) {
                result.add(jsonArray.getShort(i));
            }
            return result;
        /**
         * 4字节有符号整型
         */
        case INT:
            for (int i = 0; i < jsonArray.size(); i++) {
                result.add(jsonArray.getInteger(i));
            }
            return result;
        /**
         * 单精度浮点
         */
        case FLOAT:
            for (int i = 0; i < jsonArray.size(); i++) {
                result.add(jsonArray.getFloat(i));
            }
            return result;
        /**
         * 固定长度字符串
         */
        case CHAR:
            for (int i = 0; i < jsonArray.size(); i++) {
                result.add(new Char(jsonArray.getString(i),
                        ((CharTypeInfo) arrayTypeInfo.getElementTypeInfo()).getLength()));
            }
            return result;
        /**
         * 可变长度字符串
         */
        case VARCHAR:
            for (int i = 0; i < jsonArray.size(); i++) {
                result.add(new Varchar(jsonArray.getString(i),
                        ((VarcharTypeInfo) arrayTypeInfo.getElementTypeInfo()).getLength()));
            }
            return result;
        /**
         * 时间类型
         */
        case DATE:
            // TODO string -> date need timezone
            // TODO how to use odps Record
            for (int i = 0; i < jsonArray.size(); i++) {
                result.add(java.sql.Date.valueOf(jsonArray.getString(i)));
            }
            return result;
        /**
         * 时间戳
         */
        case TIMESTAMP:
            for (int i = 0; i < jsonArray.size(); i++) {
                result.add(Timestamp.valueOf(jsonArray.getString(i)));
            }
            return result;
        /**
         * 字节数组
         */
        case BINARY:
            for (int i = 0; i < jsonArray.size(); i++) {
                result.add(Base64.decodeBase64(jsonArray.getString(i)));
            }
            return result;
        /**
         * 日期间隔
         */
        case INTERVAL_DAY_TIME:
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject json = jsonArray.getJSONObject(i);
                result.add(new IntervalDayTime(json.getInteger("totalSeconds"), json.getInteger("nanos")));
            }
            return result;
        /**
         * 年份间隔
         */
        case INTERVAL_YEAR_MONTH:
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject json = jsonArray.getJSONObject(i);
                result.add(new IntervalYearMonth(json.getInteger("years"), json.getInteger("months")));
            }
            return result;
        /**
         * 结构体
         */
        case STRUCT:
            for (int i = 0; i < jsonArray.size(); i++) {
                result.add(
                        parseStruct(jsonArray.getJSONObject(i), (StructTypeInfo) arrayTypeInfo.getElementTypeInfo()));
            }
            return result;
        /**
         * MAP类型
         */
        case MAP:
            for (int i = 0; i < jsonArray.size(); i++) {
                result.add(parseMap(jsonArray.getJSONObject(i), (MapTypeInfo) arrayTypeInfo.getElementTypeInfo()));
            }
            return result;
        /**
         * ARRAY类型
         */
        case ARRAY:
            for (int i = 0; i < jsonArray.size(); i++) {
                result.add(parseArray(jsonArray.getJSONArray(i), (ArrayTypeInfo) arrayTypeInfo.getElementTypeInfo()));
            }
            return result;

        default:
            return result;
        }
    }

    private Map parseMap(JSONObject json, MapTypeInfo typeInfo) throws ParseException {
        if (json == null) {
            return null;
        }
        Map<Object, String> keyMap = new HashMap();
        Set<String> keys = json.keySet();
        switch (typeInfo.getKeyTypeInfo().getOdpsType()) {
        case BIGINT:
            for (String item : keys) {
                keyMap.put(Long.parseLong(item), item);
            }
            break;
        /**
         * 双精度浮点
         */
        case DOUBLE:
            for (String item : keys) {
                keyMap.put(Double.parseDouble(item), item);
            }
            break;
        /**
         * 布尔型
         */
        case BOOLEAN:
            for (String item : keys) {
                keyMap.put(Boolean.parseBoolean(item), item);
            }
            break;
        /**
         * 日期类型
         */
        case DATETIME:
            // TODO 精度
            for (String item : keys) {
                keyMap.put(dateFormat.parse(item), item);
            }
            break;
        /**
         * 字符串类型
         */
        case STRING:
            for (String item : keys) {
                keyMap.put(item, item);
            }
            break;
        /**
         * 精确小数类型
         */
        case DECIMAL:
            for (String item : keys) {
                keyMap.put(new BigDecimal(item), item);
            }
            break;
        /**
         * 1字节有符号整型
         */
        case TINYINT:
            for (String item : keys) {
                keyMap.put(Byte.parseByte(item), item);
            }
            break;
        /**
         * 2字节有符号整型
         */
        case SMALLINT:
            for (String item : keys) {
                keyMap.put(Short.parseShort(item), item);
            }
            break;
        /**
         * 4字节有符号整型
         */
        case INT:
            for (String item : keys) {
                keyMap.put(Integer.parseInt(item), item);
            }
            break;
        /**
         * 单精度浮点
         */
        case FLOAT:
            for (String item : keys) {
                keyMap.put(Float.parseFloat(item), item);
            }
            break;
        /**
         * 固定长度字符串
         */
        case CHAR:
            for (String item : keys) {
                keyMap.put(new Char(item, ((CharTypeInfo) typeInfo.getKeyTypeInfo()).getLength()), item);
            }
            break;
        /**
         * 可变长度字符串
         */
        case VARCHAR:
            for (String item : keys) {
                keyMap.put(new Varchar(item, ((VarcharTypeInfo) typeInfo.getKeyTypeInfo()).getLength()), item);
            }
            break;
        /**
         * 时间类型
         */
        case DATE:
            // TODO string -> date need timezone
            // TODO how to use odps Record
            for (String item : keys) {
                keyMap.put(java.sql.Date.valueOf(item), item);
            }
            break;
        /**
         * 时间戳
         */
        case TIMESTAMP:
            for (String item : keys) {
                keyMap.put(Timestamp.valueOf(item), item);
            }
            break;
        /**
         * 字节数组
         */
        case BINARY:
            for (String item : keys) {
                keyMap.put(new Binary(Base64.decodeBase64(item)), item);
            }
            break;
        /**
         * 日期间隔
         */
        case INTERVAL_DAY_TIME:
            for (String item : keys) {
                JSONObject jsonObject = JSON.parseObject(item);
                keyMap.put(new IntervalDayTime(jsonObject.getInteger("totalSeconds"), jsonObject.getInteger("nanos")),
                        item);
            }
            break;
        /**
         * 年份间隔
         */
        case INTERVAL_YEAR_MONTH:
            for (String item : keys) {
                JSONObject jsonObject = JSON.parseObject(item);
                keyMap.put(new IntervalYearMonth(jsonObject.getInteger("years"), jsonObject.getInteger("months")),
                        item);
            }
            break;
        default:
            break;
        // TODO throw an exception
        }
        Map result = new HashMap();
        // process map value
        switch (typeInfo.getValueTypeInfo().getOdpsType()) {
        case BIGINT:
            for (Map.Entry<Object, String> item : keyMap.entrySet()) {
                result.put(item.getKey(), json.getLong(item.getValue()));
            }
            return result;
        /**
         * 双精度浮点
         */
        case DOUBLE:
            for (Map.Entry<Object, String> item : keyMap.entrySet()) {
                result.put(item.getKey(), json.getDouble(item.getValue()));
            }
            return result;
        /**
         * 布尔型
         */
        case BOOLEAN:
            for (Map.Entry<Object, String> item : keyMap.entrySet()) {
                result.put(item.getKey(), json.getBoolean(item.getValue()));
            }
            return result;
        /**
         * 日期类型
         */
        case DATETIME:
            // TODO 精度
            for (Map.Entry<Object, String> item : keyMap.entrySet()) {
                result.put(item.getKey(), dateFormat.parse(json.getString(item.getValue())));
            }
            return result;
        /**
         * 字符串类型
         */
        case STRING:
            for (Map.Entry<Object, String> item : keyMap.entrySet()) {
                result.put(item.getKey(), json.getString(item.getValue()));
            }
            return result;
        /**
         * 精确小数类型
         */
        case DECIMAL:
            for (Map.Entry<Object, String> item : keyMap.entrySet()) {
                result.put(item.getKey(), json.getBigDecimal(item.getValue()));
            }
            return result;
        /**
         * 1字节有符号整型
         */
        case TINYINT:
            for (Map.Entry<Object, String> item : keyMap.entrySet()) {
                result.put(item.getKey(), json.getByte(item.getValue()));
            }
            return result;
        /**
         * 2字节有符号整型
         */
        case SMALLINT:
            for (Map.Entry<Object, String> item : keyMap.entrySet()) {
                result.put(item.getKey(), json.getShort(item.getValue()));
            }
            return result;
        /**
         * 4字节有符号整型
         */
        case INT:
            for (Map.Entry<Object, String> item : keyMap.entrySet()) {
                result.put(item.getKey(), json.getInteger(item.getValue()));
            }
            return result;
        /**
         * 单精度浮点
         */
        case FLOAT:
            for (Map.Entry<Object, String> item : keyMap.entrySet()) {
                result.put(item.getKey(), json.getFloat(item.getValue()));
            }
            return result;
        /**
         * 固定长度字符串
         */
        case CHAR:
            for (Map.Entry<Object, String> item : keyMap.entrySet()) {
                result.put(item.getKey(), new Char(json.getString(item.getValue()),
                        ((CharTypeInfo) typeInfo.getValueTypeInfo()).getLength()));
            }
            return result;
        /**
         * 可变长度字符串
         */
        case VARCHAR:
            for (Map.Entry<Object, String> item : keyMap.entrySet()) {
                result.put(item.getKey(), new Varchar(json.getString(item.getValue()),
                        ((VarcharTypeInfo) typeInfo.getValueTypeInfo()).getLength()));
            }
            return result;
        /**
         * 时间类型
         */
        case DATE:
            // TODO string -> date need timezone
            // TODO how to use odps Record
            for (Map.Entry<Object, String> item : keyMap.entrySet()) {
                result.put(item.getKey(), java.sql.Date.valueOf(json.getString(item.getValue())));
            }
            return result;
        /**
         * 时间戳
         */
        case TIMESTAMP:
            for (Map.Entry<Object, String> item : keyMap.entrySet()) {
                result.put(item.getKey(), Timestamp.valueOf(json.getString(item.getValue())));
            }
            return result;
        /**
         * 字节数组
         */
        case BINARY:
            for (Map.Entry<Object, String> item : keyMap.entrySet()) {
                result.put(item.getKey(), new Binary(Base64.decodeBase64(json.getString(item.getValue()))));
            }
            return result;
        /**
         * 日期间隔
         */
        case INTERVAL_DAY_TIME:
            for (Map.Entry<Object, String> item : keyMap.entrySet()) {
                JSONObject jsonObject = json.getJSONObject(item.getValue());
                result.put(item.getKey(),
                        new IntervalDayTime(jsonObject.getInteger("totalSeconds"), jsonObject.getInteger("nanos")));
            }
            return result;
        /**
         * 年份间隔
         */
        case INTERVAL_YEAR_MONTH:
            for (Map.Entry<Object, String> item : keyMap.entrySet()) {
                JSONObject jsonObject = json.getJSONObject(item.getValue());
                result.put(item.getKey(),
                        new IntervalYearMonth(jsonObject.getInteger("years"), jsonObject.getInteger("months")));
            }
            return result;
        /**
         * 结构体
         */
        case STRUCT:
            for (Map.Entry<Object, String> item : keyMap.entrySet()) {
                result.put(item.getKey(),
                        parseStruct(json.getJSONObject(item.getValue()), (StructTypeInfo) typeInfo.getValueTypeInfo()));
            }
            return result;
        /**
         * MAP类型
         */
        case MAP:
            for (Map.Entry<Object, String> item : keyMap.entrySet()) {
                result.put(item.getKey(),
                        parseMap(json.getJSONObject(item.getValue()), (MapTypeInfo) typeInfo.getValueTypeInfo()));
            }
            return result;
        /**
         * ARRAY类型
         */
        case ARRAY:
            for (Map.Entry<Object, String> item : keyMap.entrySet()) {
                result.put(item.getKey(),
                        parseArray(json.getJSONArray(item.getValue()), (ArrayTypeInfo) typeInfo.getValueTypeInfo()));
            }
            return result;

        default:
            throw new IllegalArgumentException("decode record failed. column type: " + typeInfo.getTypeName());
        }
    }

    public Struct parseStruct(JSONObject json, StructTypeInfo struct) throws ParseException {
        if (null == json) {
            return null;
        }
        List<String> fieldNames = struct.getFieldNames();
        List<TypeInfo> typeInfos = struct.getFieldTypeInfos();
        List<Object> structValues = new ArrayList<Object>();
        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            switch (typeInfos.get(i).getOdpsType()) {
            case BIGINT:
                structValues.add(json.getLong(fieldName));
                break;
            /**
             * 双精度浮点
             */
            case DOUBLE:
                structValues.add(json.getDouble(fieldName));
                break;
            /**
             * 布尔型
             */
            case BOOLEAN:
                structValues.add(json.getBoolean(fieldName));
                break;
            /**
             * 日期类型
             */
            case DATETIME:
                // TODO 精度
                structValues.add(dateFormat.parse(json.getString(fieldName)));
                break;
            /**
             * 字符串类型
             */
            case STRING:
                structValues.add(json.getString(fieldName));
                break;
            /**
             * 精确小数类型
             */
            case DECIMAL:
                structValues.add(json.getBigDecimal(fieldName));
                break;
            /**
             * 1字节有符号整型
             */
            case TINYINT:
                structValues.add(json.getByte(fieldName));
                break;
            /**
             * 2字节有符号整型
             */
            case SMALLINT:
                structValues.add(json.getShort(fieldName));
                break;
            /**
             * 4字节有符号整型
             */
            case INT:
                structValues.add(json.getInteger(fieldName));
                break;
            /**
             * 单精度浮点
             */
            case FLOAT:
                structValues.add(json.getFloat(fieldName));
                break;
            /**
             * 固定长度字符串
             */
            case CHAR:
                structValues.add(new Char(json.getString(fieldName), ((CharTypeInfo) typeInfos.get(i)).getLength()));
                break;
            /**
             * 可变长度字符串
             */
            case VARCHAR:
                structValues
                        .add(new Varchar(json.getString(fieldName), ((VarcharTypeInfo) typeInfos.get(i)).getLength()));
                break;
            /**
             * 时间类型
             */
            case DATE:
                // TODO string -> date need timezone
                // TODO how to use odps Record
                structValues.add(java.sql.Date.valueOf(json.getString(fieldName)));
                break;
            /**
             * 时间戳
             */
            case TIMESTAMP:
                structValues.add(Timestamp.valueOf(json.getString(fieldName)));
                break;
            /**
             * 字节数组
             */
            case BINARY:
                structValues.add(Base64.decodeBase64(json.getString(fieldName)));
                break;
            /**
             * 日期间隔
             */
            case INTERVAL_DAY_TIME:
                // TODO special process as map object
                structValues.add(new IntervalDayTime(json.getInteger("totalSeconds"), json.getInteger("nanos")));
                /**
                 * 年份间隔
                 */
            case INTERVAL_YEAR_MONTH:
                structValues.add(new IntervalYearMonth(json.getInteger("years"), json.getInteger("months")));
                /**
                 * 结构体
                 */
            case STRUCT:
                structValues.add(parseStruct(json.getJSONObject(fieldName), (StructTypeInfo) typeInfos.get(i)));
                break;
            /**
             * MAP类型
             */
            case MAP:
                structValues.add(parseMap(json.getJSONObject(fieldName), (MapTypeInfo) typeInfos.get(i)));
                break;
            /**
             * ARRAY类型
             */
            case ARRAY:
                structValues.add(parseArray(json.getJSONArray(fieldName), (ArrayTypeInfo) typeInfos.get(i)));
                break;
            }
        }

        SimpleStruct simpleStruct = new SimpleStruct(struct, structValues);
        return simpleStruct;
    }

    public Long getLastActiveTime() {
        return lastActiveTime;
    }

    public void setLastActiveTime(Long lastActiveTime) {
        this.lastActiveTime = lastActiveTime;
    }

    public Long getCurrentTotalBytes() throws IOException {
        return this.protobufRecordPack.getTotalBytes();
    }
}
