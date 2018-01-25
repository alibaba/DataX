package com.alibaba.datax.plugin.writer.hbase094xwriter;

import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class NormalTask extends HbaseAbstractTask {
    private static final Logger LOG = LoggerFactory.getLogger(NormalTask.class);
    public NormalTask(Configuration configuration) {
        super(configuration);
    }

    @Override
    public Put convertRecordToPut(Record record){
        byte[] rowkey = getRowkey(record);
        Put put = null;
        if(this.versionColumn == null){
            put = new Put(rowkey);
            put.setWriteToWAL(super.walFlag);
        }else {
            long timestamp = getVersion(record);
            put = new Put(rowkey,timestamp);
        }
        for (Configuration aColumn : columns) {
            Integer index = aColumn.getInt(Key.INDEX);
            String type = aColumn.getString(Key.TYPE);
            ColumnType columnType = ColumnType.getByTypeName(type);
            String name = aColumn.getString(Key.NAME);
            String promptInfo = "Hbasewriter 中，column 的列配置格式应该是：列族:列名. 您配置的列错误：" + name;
            String[] cfAndQualifier = name.split(":");
            Validate.isTrue(cfAndQualifier != null && cfAndQualifier.length == 2
                    && StringUtils.isNotBlank(cfAndQualifier[0])
                    && StringUtils.isNotBlank(cfAndQualifier[1]), promptInfo);
            if(index >= record.getColumnNumber()){
                throw DataXException.asDataXException(Hbase094xWriterErrorCode.ILLEGAL_VALUE, String.format("您的column配置项中中index值超出范围,根据reader端配置,index的值小于%s,而您配置的值为%s，请检查并修改.",record.getColumnNumber(),index));
            }
            byte[] columnBytes = getColumnByte(columnType,record.getColumn(index));
            //columnBytes 为null忽略这列
            if(null != columnBytes){
                put.add(Bytes.toBytes(
                        cfAndQualifier[0]),
                        Bytes.toBytes(cfAndQualifier[1]),
                        columnBytes);
            }else{
                continue;
            }
        }
        return put;
    }

    public byte[] getRowkey(Record record){
        byte[] rowkeyBuffer  = {};
        for (Configuration aRowkeyColumn : rowkeyColumn) {
            Integer index = aRowkeyColumn.getInt(Key.INDEX);
            String type = aRowkeyColumn.getString(Key.TYPE);
            ColumnType columnType = ColumnType.getByTypeName(type);
            if(index == -1){
                String value = aRowkeyColumn.getString(Key.VALUE);
                rowkeyBuffer = Bytes.add(rowkeyBuffer,getValueByte(columnType,value));
            }else{
                if(index >= record.getColumnNumber()){
                    throw DataXException.asDataXException(Hbase094xWriterErrorCode.CONSTRUCT_ROWKEY_ERROR, String.format("您的rowkeyColumn配置项中中index值超出范围,根据reader端配置,index的值小于%s,而您配置的值为%s，请检查并修改.",record.getColumnNumber(),index));
                }
                byte[] value = getColumnByte(columnType,record.getColumn(index));
                rowkeyBuffer = Bytes.add(rowkeyBuffer, value);
            }
        }
        return rowkeyBuffer;
    }

    public long getVersion(Record record){
        int index = versionColumn.getInt(Key.INDEX);
        long timestamp;
        if(index == -1){
            //指定时间作为版本
            timestamp = versionColumn.getLong(Key.VALUE);
            if(timestamp < 0){
                throw DataXException.asDataXException(Hbase094xWriterErrorCode.CONSTRUCT_VERSION_ERROR, "您指定的版本非法!");
            }
        }else{
            //指定列作为版本,long/doubleColumn直接record.aslong, 其它类型尝试用yyyy-MM-dd HH:mm:ss,yyyy-MM-dd HH:mm:ss SSS去format
            if(index >= record.getColumnNumber()){
                throw DataXException.asDataXException(Hbase094xWriterErrorCode.CONSTRUCT_VERSION_ERROR, String.format("您的versionColumn配置项中中index值超出范围,根据reader端配置,index的值小于%s,而您配置的值为%s，请检查并修改.",record.getColumnNumber(),index));
            }
            if(record.getColumn(index).getRawData()  == null){
                throw DataXException.asDataXException(Hbase094xWriterErrorCode.CONSTRUCT_VERSION_ERROR, "您指定的版本为空!");
            }
            SimpleDateFormat df_senconds = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            SimpleDateFormat df_ms = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
            if(record.getColumn(index) instanceof LongColumn || record.getColumn(index) instanceof DoubleColumn){
                timestamp = record.getColumn(index).asLong();
            }else {
                Date date;
                try{
                    date = df_ms.parse(record.getColumn(index).asString());
                }catch (ParseException e){
                    try {
                        date = df_senconds.parse(record.getColumn(index).asString());
                    } catch (ParseException e1) {
                        LOG.info(String.format("您指定第[%s]列作为hbase写入版本,但在尝试用yyyy-MM-dd HH:mm:ss 和 yyyy-MM-dd HH:mm:ss SSS 去解析为Date时均出错,请检查并修改",index));
                        throw DataXException.asDataXException(Hbase094xWriterErrorCode.CONSTRUCT_VERSION_ERROR, e1);
                    }
                }
                timestamp = date.getTime();
            }
        }
        return timestamp;
    }
}