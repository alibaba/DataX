package com.alibaba.datax.plugin.reader.odpsreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.MessageSource;
import com.alibaba.datax.plugin.reader.odpsreader.util.OdpsUtil;
import com.alibaba.fastjson2.JSON;
import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.*;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ReaderProxy {
    private static final Logger LOG = LoggerFactory
            .getLogger(ReaderProxy.class);
    private static final MessageSource MESSAGE_SOURCE = MessageSource.loadResourceBundle(ReaderProxy.class);
    private static boolean IS_DEBUG = LOG.isDebugEnabled();

    private RecordSender recordSender;
    private TableTunnel.DownloadSession downloadSession;
    private Map<String, TypeInfo> columnTypeMap;
    private List<InternalColumnInfo> parsedColumns;
    private String partition;
    private boolean isPartitionTable;

    private long start;
    private long count;
    private boolean isCompress;
    
    private static final String NULL_INDICATOR = null;
    // TODO 没有支持用户可配置
    // TODO 没有timezone
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

    public ReaderProxy(RecordSender recordSender, TableTunnel.DownloadSession downloadSession,
                       Map<String, TypeInfo> columnTypeMap,
                       List<InternalColumnInfo> parsedColumns, String partition,
                       boolean isPartitionTable, long start, long count, boolean isCompress, Configuration taskConfig) {
        this.recordSender = recordSender;
        this.downloadSession = downloadSession;
        this.columnTypeMap = columnTypeMap;
        this.parsedColumns = parsedColumns;
        this.partition = partition;
        this.isPartitionTable = isPartitionTable;
        this.start = start;
        this.count = count;
        this.isCompress = isCompress;
        
        this.calendarForDate = this.initCalendar(taskConfig);
        this.useDateWithCalendar = taskConfig.getBool("useDateWithCalendar", true);
    }

    // warn: odps 分区列和正常列不能重名, 所有列都不不区分大小写
    public void doRead() {
        try {
            LOG.info("start={}, count={}",start, count);
            List<Column> userConfigNormalColumns = OdpsUtil.getNormalColumns(this.parsedColumns, this.columnTypeMap);
            RecordReader recordReader = null;
            // fix #ODPS-52184/10332469, updateColumnsSize表示如果用户指定的读取源表列数100列以内的话,则进行列裁剪优化;
            int updateColumnsSize = 100;
            if(userConfigNormalColumns.size() <= updateColumnsSize){
                recordReader = OdpsUtil.getRecordReader(downloadSession, start, count, isCompress, userConfigNormalColumns);
            } else {
                recordReader = OdpsUtil.getRecordReader(downloadSession, start, count, isCompress);
            }

            Record odpsRecord;
            Map<String, String> partitionMap = this
                    .parseCurrentPartitionValue();

            int retryTimes = 1;
            while (true) {
                try {
                    odpsRecord = recordReader.read();
                } catch(Exception e) {
                    //odps read 异常后重试10次
                    LOG.warn("warn : odps read exception: {}", e.getMessage());
                    if(retryTimes < 10) {
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException ignored) {
                        }
                        recordReader = downloadSession.openRecordReader(start, count, isCompress);
                        LOG.warn(MESSAGE_SOURCE.message("readerproxy.1", retryTimes));
                        retryTimes++;
                        continue;
                    } else {
                        throw DataXException.asDataXException(OdpsReaderErrorCode.ODPS_READ_EXCEPTION, e);
                    }
                }
                //记录已经读取的点
                start++;
                count--;

                if (odpsRecord != null) {

                    com.alibaba.datax.common.element.Record dataXRecord = recordSender
                            .createRecord();
                    // warn: for PARTITION||NORMAL columnTypeMap's key
                    // sets(columnName) is big than parsedColumns's left
                    // sets(columnName), always contain
                    for (InternalColumnInfo pair : this.parsedColumns) {
                        String columnName = pair.getColumnName();
                        switch (pair.getColumnType()) {
                            case PARTITION:
                                String partitionColumnValue = this
                                        .getPartitionColumnValue(partitionMap,
                                                columnName);
                                this.odpsColumnToDataXField(odpsRecord, dataXRecord,
                                        this.columnTypeMap.get(columnName),
                                        partitionColumnValue, true);
                                break;
                            case NORMAL:
                                this.odpsColumnToDataXField(odpsRecord, dataXRecord,
                                        this.columnTypeMap.get(columnName), columnName,
                                        false);
                                break;
                            case CONSTANT:
                                dataXRecord.addColumn(new StringColumn(columnName));
                                break;
                            default:
                                break;
                        }
                    }
                    recordSender.sendToWriter(dataXRecord);
                } else {
                    break;
                }
            }
            //fixed, 避免recordReader.close失败，跟鸣天确认过，可以不用关闭RecordReader
            try {
                recordReader.close();
            } catch (Exception e) {
                LOG.warn("recordReader close exception", e);
            }
        } catch (DataXException e) {
            throw e;
        } catch (Exception e) {
            // warn: if dirty
            throw DataXException.asDataXException(
                    OdpsReaderErrorCode.READ_DATA_FAIL, e);
        }
    }

    private Map<String, String> parseCurrentPartitionValue() {
        Map<String, String> partitionMap = new HashMap<String, String>();
        if (this.isPartitionTable) {
            String[] splitedPartition = this.partition.split(",");
            for (String eachPartition : splitedPartition) {
                String[] partitionDetail = eachPartition.split("=");
                // warn: check partition like partition=1
                if (2 != partitionDetail.length) {
                    throw DataXException
                            .asDataXException(
                                    OdpsReaderErrorCode.ILLEGAL_VALUE,
                                    MESSAGE_SOURCE.message("readerproxy.2", eachPartition));
                }
                // warn: translate to lower case, it's more comfortable to
                // compare whit user's input columns
                String partitionName = partitionDetail[0].toLowerCase();
                String partitionValue = partitionDetail[1];
                partitionMap.put(partitionName, partitionValue);
            }
        }
        if (IS_DEBUG) {
            LOG.debug(String.format("partition value details: %s",
                    com.alibaba.fastjson2.JSON.toJSONString(partitionMap)));
        }
        return partitionMap;
    }

    private String getPartitionColumnValue(Map<String, String> partitionMap,
            String partitionColumnName) {
        // warn: to lower case
        partitionColumnName = partitionColumnName.toLowerCase();
        // it's will never happen, but add this checking
        if (!partitionMap.containsKey(partitionColumnName)) {
            String errorMessage = MESSAGE_SOURCE.message("readerproxy.3",
                    com.alibaba.fastjson2.JSON.toJSONString(partitionMap),
                    partitionColumnName);
            throw DataXException.asDataXException(
                    OdpsReaderErrorCode.READ_DATA_FAIL, errorMessage);
        }
        return partitionMap.get(partitionColumnName);
    }

    /**
     * TODO warn: odpsRecord 的 String 可能获取出来的是 binary
     * 
     * warn: there is no dirty data in reader plugin, so do not handle dirty
     * data with TaskPluginCollector
     * 
     * warn: odps only support BIGINT && String partition column actually
     * 
     * @param odpsRecord
     *            every line record of odps table
     * @param dataXRecord
     *            every datax record, to be send to writer. method getXXX() case sensitive
     * @param typeInfo
     *            odps column type
     * @param columnNameValue
     *            for partition column it's column value, for normal column it's
     *            column name
     * @param isPartitionColumn
     *            true means partition column and false means normal column
     * */
    private void odpsColumnToDataXField(Record odpsRecord,
            com.alibaba.datax.common.element.Record dataXRecord, TypeInfo typeInfo,
            String columnNameValue, boolean isPartitionColumn) {
        
        ArrayRecord record = (ArrayRecord) odpsRecord;
        
        OdpsType type = typeInfo.getOdpsType();
        
        switch (type) {
            case BIGINT: {
                if (isPartitionColumn) {
                    dataXRecord.addColumn(new LongColumn(columnNameValue));
                } else {
                    dataXRecord.addColumn(new LongColumn(record
                            .getBigint(columnNameValue)));
                }
                break;
            }
            case BOOLEAN: {
                if (isPartitionColumn) {
                    dataXRecord.addColumn(new BoolColumn(columnNameValue));
                } else {
                    dataXRecord.addColumn(new BoolColumn(record
                            .getBoolean(columnNameValue)));
                }
                break;
            }
            case DATE:
            case DATETIME: {
                // odps分区列，目前支持TINYINT、SMALLINT、INT、BIGINT、VARCHAR和STRING类型
                if (isPartitionColumn) {
                    try {
                        dataXRecord.addColumn(new DateColumn(ColumnCast
                                .string2Date(new StringColumn(columnNameValue))));
                    } catch (ParseException e) {
                        String errMessage = MESSAGE_SOURCE.message("readerproxy.4",
                                this.partition, columnNameValue);
                        LOG.error(errMessage);
                        throw DataXException.asDataXException(
                                OdpsReaderErrorCode.READ_DATA_FAIL, errMessage, e);
                    }
                } else {
                    if (com.aliyun.odps.OdpsType.DATETIME == type) {
                        dataXRecord.addColumn(new DateColumn(record
                                .getDatetime(columnNameValue)));
                    } else {
                        if (this.useDateWithCalendar) {
                            dataXRecord.addColumn(new DateColumn(record.
                                    getDate(columnNameValue, this.calendarForDate)));
                        } else {
                            dataXRecord.addColumn(new DateColumn(record
                                    .getDate(columnNameValue)));
                        }
                        
                    }
                }

                break;
            }
            case DOUBLE: {
                if (isPartitionColumn) {
                    dataXRecord.addColumn(new DoubleColumn(columnNameValue));
                } else {
                    dataXRecord.addColumn(new DoubleColumn(record
                            .getDouble(columnNameValue)));
                }
                break;
            }
            case DECIMAL: {
                if(isPartitionColumn) {
                    dataXRecord.addColumn(new DoubleColumn(columnNameValue));
                } else {
                    dataXRecord.addColumn(new DoubleColumn(record.getDecimal(columnNameValue)));
                }
                break;
            }
            case STRING: {
                if (isPartitionColumn) {
                    dataXRecord.addColumn(new StringColumn(columnNameValue));
                } else {
                    dataXRecord.addColumn(new StringColumn(record
                            .getString(columnNameValue)));
                }
                break;
            }
            case TINYINT:
                if (isPartitionColumn) {
                    dataXRecord.addColumn(new LongColumn(columnNameValue));
                } else {
                    Byte value = record.getTinyint(columnNameValue);
                    Integer intValue = value != null ? value.intValue() : null;
                    dataXRecord.addColumn(new LongColumn(intValue));
                }
                break;
            case SMALLINT: {
                if (isPartitionColumn) {
                    dataXRecord.addColumn(new LongColumn(columnNameValue));
                } else {
                    Short value = record.getSmallint(columnNameValue);
                    Long valueInLong = null;
                    if (null != value) {
                        valueInLong = value.longValue();
                    }
                    dataXRecord.addColumn(new LongColumn(valueInLong));
                }
                break;
            }
            case INT: {
                if (isPartitionColumn) {
                    dataXRecord.addColumn(new LongColumn(columnNameValue));
                } else {
                    dataXRecord.addColumn(new LongColumn(record
                        .getInt(columnNameValue)));
                }
                break;
            }
            case FLOAT: {
                if (isPartitionColumn) {
                    dataXRecord.addColumn(new DoubleColumn(columnNameValue));
                } else {
                    dataXRecord.addColumn(new DoubleColumn(record
                        .getFloat(columnNameValue)));
                }
                break;
            }
            case VARCHAR: {
                if (isPartitionColumn) {
                    dataXRecord.addColumn(new StringColumn(columnNameValue));
                } else {
                    Varchar value = record.getVarchar(columnNameValue);
                    String columnValue = value != null ? value.getValue() : null;
                    dataXRecord.addColumn(new StringColumn(columnValue));
                }
                break;
            }
            case TIMESTAMP: {
                if (isPartitionColumn) {
                    try {
                        dataXRecord.addColumn(new DateColumn(ColumnCast
                            .string2Date(new StringColumn(columnNameValue))));
                    } catch (ParseException e) {
                        String errMessage = MESSAGE_SOURCE.message("readerproxy.4",
                            this.partition, columnNameValue);
                        LOG.error(errMessage);
                        throw DataXException.asDataXException(
                            OdpsReaderErrorCode.READ_DATA_FAIL, errMessage, e);
                    }
                } else {
                    dataXRecord.addColumn(new DateColumn(record
                        .getTimestamp(columnNameValue)));
                }

                break;
            }
            case BINARY: {
                if (isPartitionColumn) {
                    dataXRecord.addColumn(new BytesColumn(columnNameValue.getBytes()));
                } else {
//                    dataXRecord.addColumn(new BytesColumn(record
//                        .getBinary(columnNameValue).data()));
                    Binary binaryData = record.getBinary(columnNameValue);
                    if (null == binaryData) {
                        dataXRecord.addColumn(new BytesColumn(null));
                    } else {
                        dataXRecord.addColumn(new BytesColumn(binaryData.data()));
                    }
                }
                break;
            }
            case ARRAY: {
                if (isPartitionColumn) {
                    dataXRecord.addColumn(new StringColumn(columnNameValue));
                } else {
                    List arrayValue = record.getArray(columnNameValue);
                    if (arrayValue == null) {
                        dataXRecord.addColumn(new StringColumn(null));
                    } else {
                        dataXRecord.addColumn(new StringColumn(JSON.toJSONString(transOdpsArrayToJavaList(arrayValue, (ArrayTypeInfo)typeInfo))));
                    }
                }
                break;
            }
            case MAP: {
                if (isPartitionColumn) {
                    dataXRecord.addColumn(new StringColumn(columnNameValue));
                } else {
                    Map mapValue = record.getMap(columnNameValue);
                    if (mapValue == null) {
                        dataXRecord.addColumn(new StringColumn(null));
                    } else {
                        dataXRecord.addColumn(new StringColumn(JSON.toJSONString(transOdpsMapToJavaMap(mapValue, (MapTypeInfo)typeInfo))));
                    }
                }
                break;
            }
            case STRUCT: {
                if (isPartitionColumn) {
                    dataXRecord.addColumn(new StringColumn(columnNameValue));
                } else {
                    Struct structValue = record.getStruct(columnNameValue);
                    if (structValue == null) {
                        dataXRecord.addColumn(new StringColumn(null));
                    } else {
                        dataXRecord.addColumn(new StringColumn(JSON.toJSONString(transOdpsStructToJavaMap(structValue))));
                    }
                }
                break;
            }
            default:
                throw DataXException.asDataXException(
                        OdpsReaderErrorCode.ILLEGAL_VALUE,
                        MESSAGE_SOURCE.message("readerproxy.5", type));
        }
    }
    
    private List transOdpsArrayToJavaList(List odpsArray, ArrayTypeInfo typeInfo) {
        TypeInfo eleType = typeInfo.getElementTypeInfo();
        List result = new ArrayList();
        switch (eleType.getOdpsType()) {
          // warn:array<double> [1.2, 3.4]  被转为了："["1.2", "3.4"]"， 本来应该被转换成 "[1.2, 3.4]"
          // 注意回归Case覆盖
          case BIGINT:
          case DOUBLE:
          case INT:
          case FLOAT:
          case DECIMAL:
          case TINYINT:
          case SMALLINT:
              for (Object item : odpsArray) {
                  Object object = item;
                  result.add(object == null ? NULL_INDICATOR : object);
                }
                return result;
          case BOOLEAN: // 未调整array<Boolean> 问题
          case STRING:
          case VARCHAR:
          case CHAR:
          case TIMESTAMP:
          case DATE:
            for (Object item : odpsArray) {
              Object object = item;
              result.add(object == null ? NULL_INDICATOR : object.toString());
            }
            return result;
          /**
           * 日期类型
           */
          case DATETIME:
            for (Object item : odpsArray) {
              Date dateVal = (Date) item;
              result.add(dateVal == null ? NULL_INDICATOR : dateFormat.format(dateVal));
            }
            return result;
          /**
           * 字节数组
           */
          case BINARY:
            for (Object item : odpsArray) {
              Binary binaryVal = (Binary) item;
              result.add(binaryVal == null ? NULL_INDICATOR :
                  Base64.encodeBase64(binaryVal.data()));
            }
            return result;
          /**
           * 日期间隔
           */
          case INTERVAL_DAY_TIME:
            for (Object item : odpsArray) {
              IntervalDayTime dayTimeVal = (IntervalDayTime) item;
              result.add(dayTimeVal == null ? NULL_INDICATOR :
                  transIntervalDayTimeToJavaMap(dayTimeVal));
            }
            return result;
          /**
           * 年份间隔
           */
          case INTERVAL_YEAR_MONTH:
            for (Object item : odpsArray) {
              IntervalYearMonth yearMonthVal = (IntervalYearMonth) item;
              result.add(yearMonthVal == null ? NULL_INDICATOR :
                  transIntervalYearMonthToJavaMap(yearMonthVal));
            }
            return result;
          /**
           * 结构体
           */
          case STRUCT:
            for (Object item : odpsArray) {
              Struct structVal = (Struct) item;
              result.add(structVal == null ? NULL_INDICATOR :
                  transOdpsStructToJavaMap(structVal));
            }
            return result;
          /**
           * MAP类型
           */
          case MAP:
            for (Object item : odpsArray) {
              Map mapVal = (Map) item;
              result.add(mapVal == null ? NULL_INDICATOR :
                  transOdpsMapToJavaMap(mapVal, (MapTypeInfo) eleType));
            }
            return result;
          /**
           * ARRAY类型
           */
          case ARRAY:
            for (Object item : odpsArray) {
              List arrayVal = (List) item;
              result.add(arrayVal == null ? NULL_INDICATOR :
                  transOdpsArrayToJavaList(arrayVal, (ArrayTypeInfo) eleType));
            }
            return result;
          default:
            throw new IllegalArgumentException("decode record failed. column type: " +  eleType.getTypeName());
        }
      }

    private Map transOdpsMapToJavaMap(Map odpsMap, MapTypeInfo typeInfo) {
        TypeInfo keyType = typeInfo.getKeyTypeInfo();
        TypeInfo valueType = typeInfo.getValueTypeInfo();
        Map result = new HashMap();
        Set<Map.Entry> entrySet = null;
        switch (valueType.getOdpsType()) {
          case BIGINT:
          case DOUBLE:
          case BOOLEAN:
          case STRING:
          case DECIMAL:
          case TINYINT:
          case SMALLINT:
          case INT:
          case FLOAT:
          case CHAR:
          case VARCHAR:
          case DATE:
          case TIMESTAMP:
            switch (keyType.getOdpsType()) {
              case DATETIME:
                entrySet = odpsMap.entrySet();
                for (Map.Entry item : entrySet) {
                  Object value = item.getValue();
                  result.put(dateFormat.format((Date)item.getKey()), value == null ? NULL_INDICATOR : value.toString());
                }
                return result;
              case BINARY:
                entrySet = odpsMap.entrySet();
                for (Map.Entry item : entrySet) {
                  Object value = item.getValue();
                  result.put(Base64.encodeBase64(((Binary)item.getKey()).data()),
                      value == null ? NULL_INDICATOR : value.toString());
                }
                return result;
              default:
                entrySet = odpsMap.entrySet();
                for (Map.Entry item : entrySet) {
                  Object value = item.getValue();
                  result.put(item.getKey(), value == null ? NULL_INDICATOR : value.toString());
                }
                return result;
            }
          /**
           * 日期类型
           */
          case DATETIME:
            switch (keyType.getOdpsType()) {
              case DATETIME:
                entrySet = odpsMap.entrySet();
                for (Map.Entry item : entrySet) {
                  Date dateVal = (Date) item.getValue();
                  result.put(dateFormat.format((Date)item.getKey()),
                      dateVal == null ? NULL_INDICATOR : dateFormat.format(dateVal));
                }
                return result;
              case BINARY:
                entrySet = odpsMap.entrySet();
                for (Map.Entry item : entrySet) {
                  Date dateVal = (Date) item.getValue();
                  result.put(Base64.encodeBase64(((Binary)item.getKey()).data()),
                     dateVal == null ? NULL_INDICATOR : dateFormat.format(dateVal));
                }
                return result;
              default:
                entrySet = odpsMap.entrySet();
                for (Map.Entry item : entrySet) {
                  Date dateVal = (Date) item.getValue();
                  result.put(item.getKey(), dateVal == null ? NULL_INDICATOR : dateFormat.format(dateVal));
                }
                return result;
            }
          /**
           * 字节数组
           */
          case BINARY:
            switch (keyType.getOdpsType()) {
              case DATETIME:
                entrySet = odpsMap.entrySet();
                for (Map.Entry item : entrySet) {
                  Binary binaryVal = (Binary) item.getValue();
                  result.put(dateFormat.format((Date)item.getKey()),  binaryVal == null ? NULL_INDICATOR :
                      Base64.encodeBase64(binaryVal.data()));
                }
                return result;
              case BINARY:
                entrySet = odpsMap.entrySet();
                for (Map.Entry item : entrySet) {
                  Binary binaryVal = (Binary) item.getValue();
                  result.put(Base64.encodeBase64(((Binary)item.getKey()).data()),
                      binaryVal == null ? NULL_INDICATOR :
                          Base64.encodeBase64(binaryVal.data()));
                }
                return result;
              default:
                entrySet = odpsMap.entrySet();
                for (Map.Entry item : entrySet) {
                  Binary binaryVal = (Binary) item.getValue();
                  result.put(item.getKey(),  binaryVal == null ? NULL_INDICATOR :
                      Base64.encodeBase64(binaryVal.data()));
                }
                return result;
            }
          /**
           * 日期间隔
           */
          case INTERVAL_DAY_TIME:
            switch (keyType.getOdpsType()) {
              case DATETIME:
                entrySet = odpsMap.entrySet();
                for (Map.Entry item : entrySet) {
                  IntervalDayTime dayTimeVal = (IntervalDayTime) item.getValue();
                  result.put(dateFormat.format((Date)item.getKey()), dayTimeVal == null ? NULL_INDICATOR :
                      transIntervalDayTimeToJavaMap(dayTimeVal));
                }
                return result;
              case BINARY:
                entrySet = odpsMap.entrySet();
                for (Map.Entry item : entrySet) {
                  IntervalDayTime dayTimeVal = (IntervalDayTime) item.getValue();
                  result.put(Base64.encodeBase64(((Binary)item.getKey()).data()),
                      dayTimeVal == null ? NULL_INDICATOR :
                          transIntervalDayTimeToJavaMap(dayTimeVal));
                }
                return result;
              default:
                entrySet = odpsMap.entrySet();
                for (Map.Entry item : entrySet) {
                  IntervalDayTime dayTimeVal = (IntervalDayTime) item.getValue();
                  result.put(item.getKey(), dayTimeVal == null ? NULL_INDICATOR :
                      transIntervalDayTimeToJavaMap(dayTimeVal));
                }
                return result;
            }
          /**
           * 年份间隔
           */
          case INTERVAL_YEAR_MONTH:
            switch (keyType.getOdpsType()) {
              case DATETIME:
                entrySet = odpsMap.entrySet();
                for (Map.Entry item : entrySet) {
                  IntervalYearMonth yearMonthVal = (IntervalYearMonth) item.getValue();
                  result.put(dateFormat.format((Date)item.getKey()), yearMonthVal == null ? NULL_INDICATOR :
                      transIntervalYearMonthToJavaMap(yearMonthVal));
                }
                return result;
              case BINARY:
                entrySet = odpsMap.entrySet();
                for (Map.Entry item : entrySet) {
                  IntervalYearMonth yearMonthVal = (IntervalYearMonth) item.getValue();
                  result.put(Base64.encodeBase64(((Binary)item.getKey()).data()),
                      yearMonthVal == null ? NULL_INDICATOR :
                          transIntervalYearMonthToJavaMap(yearMonthVal));
                }
                return result;
              default:
                entrySet = odpsMap.entrySet();
                for (Map.Entry item : entrySet) {
                  IntervalYearMonth yearMonthVal = (IntervalYearMonth) item.getValue();
                  result.put(item.getKey(), yearMonthVal == null ? NULL_INDICATOR :
                      transIntervalYearMonthToJavaMap(yearMonthVal));
                }
                return result;
            }
          /**
           * 结构体
           */
          case STRUCT:
            switch (keyType.getOdpsType()) {
              case DATETIME:
                entrySet = odpsMap.entrySet();
                for (Map.Entry item : entrySet) {
                  Struct structVal = (Struct) item.getValue();
                  result.put(dateFormat.format((Date)item.getKey()), structVal == null ? NULL_INDICATOR :
                      transOdpsStructToJavaMap(structVal));
                }
                return result;
              case BINARY:
                entrySet = odpsMap.entrySet();
                for (Map.Entry item : entrySet) {
                  Struct structVal = (Struct) item.getValue();
                  result.put(Base64.encodeBase64(((Binary)item.getKey()).data()),
                      structVal == null ? NULL_INDICATOR :
                          transOdpsStructToJavaMap(structVal));
                }
                return result;
              default:
                entrySet = odpsMap.entrySet();
                for (Map.Entry item : entrySet) {
                  Struct structVal = (Struct) item.getValue();
                  result.put(item.getKey(), structVal == null ? NULL_INDICATOR :
                      transOdpsStructToJavaMap(structVal));
                }
                return result;
            }
          /**
           * MAP类型
           */
          case MAP:
            switch (keyType.getOdpsType()) {
              case DATETIME:
                entrySet = odpsMap.entrySet();
                for (Map.Entry item : entrySet) {
                  Map mapVal = (Map) item.getValue();
                  result.put(dateFormat.format((Date)item.getKey()),mapVal == null ? NULL_INDICATOR :
                      transOdpsMapToJavaMap(mapVal, (MapTypeInfo) valueType));
                }
                return result;
              case BINARY:
                entrySet = odpsMap.entrySet();
                for (Map.Entry item : entrySet) {
                  Map mapVal = (Map) item.getValue();
                  result.put(Base64.encodeBase64(((Binary)item.getKey()).data()),
                      mapVal == null ? NULL_INDICATOR : transOdpsMapToJavaMap(mapVal, (MapTypeInfo) valueType));
                }
                return result;
              default:
                entrySet = odpsMap.entrySet();
                for (Map.Entry item : entrySet) {
                  Map mapVal = (Map) item.getValue();
                  result.put(item.getKey(), mapVal == null ? NULL_INDICATOR :
                      transOdpsMapToJavaMap(mapVal, (MapTypeInfo) valueType));
                }
                return result;
            }
          /**
           * ARRAY类型
           */
          case ARRAY:
            switch (keyType.getOdpsType()) {
              case DATETIME:
                entrySet = odpsMap.entrySet();
                for (Map.Entry item : entrySet) {
                  List arrayVal = (List) item.getValue();
                  result.put(dateFormat.format((Date)item.getKey()),arrayVal == null ? NULL_INDICATOR :
                      transOdpsArrayToJavaList(arrayVal, (ArrayTypeInfo) valueType));
                }
                return result;
              case BINARY:
                entrySet = odpsMap.entrySet();
                for (Map.Entry item : entrySet) {
                  List arrayVal = (List) item.getValue();
                  result.put(Base64.encodeBase64(((Binary)item.getKey()).data()),
                      arrayVal == null ? NULL_INDICATOR : transOdpsArrayToJavaList(arrayVal, (ArrayTypeInfo) valueType));
                }
                return result;
              default:
                entrySet = odpsMap.entrySet();
                for (Map.Entry item : entrySet) {
                  List arrayVal = (List) item.getValue();
                  result.put(item.getKey(), arrayVal == null ? NULL_INDICATOR :
                      transOdpsArrayToJavaList(arrayVal, (ArrayTypeInfo) valueType));
                }
                return result;
            }
          default:
            throw new IllegalArgumentException("decode record failed. column type: " +  valueType.getTypeName());
        }
      }
    
    private Map transIntervalDayTimeToJavaMap(IntervalDayTime dayTime) {
        Map<String, Long> result = new HashMap<String, Long>();
        result.put("totalSeconds", dayTime.getTotalSeconds());
        result.put("nanos", (long)dayTime.getNanos());
        return result;
      }
    
    private Map transOdpsStructToJavaMap(Struct odpsStruct) {
        Map result = new HashMap();
        for (int i = 0; i < odpsStruct.getFieldCount(); i++) {
          String fieldName = odpsStruct.getFieldName(i);
          Object fieldValue = odpsStruct.getFieldValue(i);
          TypeInfo fieldType = odpsStruct.getFieldTypeInfo(i);
          switch (fieldType.getOdpsType()) {
            case BIGINT:
            case DOUBLE:
            case BOOLEAN:
            case STRING:
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INT:
            case FLOAT:
            case VARCHAR:
            case CHAR:
            case TIMESTAMP:
            case DATE:
              result.put(fieldName, fieldValue == null ? NULL_INDICATOR : fieldValue.toString());
              break;
            /**
             * 日期类型
             */
            case DATETIME:
              Date dateVal = (Date) fieldValue;
              result.put(fieldName, dateVal == null ? NULL_INDICATOR : dateFormat.format(dateVal));
              break;
            /**
             * 字节数组
             */
            case BINARY:
              Binary binaryVal = (Binary) fieldValue;
              result.put(fieldName, binaryVal == null ? NULL_INDICATOR :
                  Base64.encodeBase64(binaryVal.data()));
              break;
            /**
             * 日期间隔
             */
            case INTERVAL_DAY_TIME:
              IntervalDayTime dayTimeVal = (IntervalDayTime) fieldValue;
              result.put(fieldName, dayTimeVal == null ? NULL_INDICATOR :
                      transIntervalDayTimeToJavaMap(dayTimeVal));
              break;
            /**
             * 年份间隔
             */
            case INTERVAL_YEAR_MONTH:
              IntervalYearMonth yearMonthVal = (IntervalYearMonth) fieldValue;
              result.put(fieldName, yearMonthVal == null ? NULL_INDICATOR :
                  transIntervalYearMonthToJavaMap(yearMonthVal));
              break;
              /**
             * 结构体
             */
            case STRUCT:
              Struct structVal = (Struct) fieldValue;
              result.put(fieldName, structVal == null ? NULL_INDICATOR :
                  transOdpsStructToJavaMap(structVal));
              break;
            /**
             * MAP类型
             */
            case MAP:
              Map mapVal = (Map) fieldValue;
              result.put(fieldName, mapVal == null ? NULL_INDICATOR :
                  transOdpsMapToJavaMap(mapVal, (MapTypeInfo) fieldType));
              break;
            /**
             * ARRAY类型
             */
            case ARRAY:
              List arrayVal = (List) fieldValue;
              result.put(fieldName, arrayVal == null ? NULL_INDICATOR :
                  transOdpsArrayToJavaList(arrayVal, (ArrayTypeInfo) fieldType));
              break;
            default:
              throw new IllegalArgumentException("decode record failed. column type: " + fieldType.getTypeName());
          }
        }

        return result;
      }
    
    private Map transIntervalYearMonthToJavaMap(IntervalYearMonth yearMonth) {
        Map <String, Integer> result = new HashMap<String, Integer>();
        result.put("years", yearMonth.getYears());
        result.put("months", yearMonth.getMonths());
        return result;
      }
    
}
