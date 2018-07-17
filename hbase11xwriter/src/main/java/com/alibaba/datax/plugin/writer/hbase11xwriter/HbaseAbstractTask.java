package com.alibaba.datax.plugin.writer.hbase11xwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

public abstract class HbaseAbstractTask {
    private final static Logger LOG = LoggerFactory.getLogger(HbaseAbstractTask.class);

    public NullModeType nullMode = null;

    public List<Configuration> columns;
    public List<Configuration> rowkeyColumn;
    public Configuration versionColumn;


    //public Table htable;
    public String encoding;
    public Boolean walFlag;
    public BufferedMutator bufferedMutator;


    public HbaseAbstractTask(com.alibaba.datax.common.util.Configuration configuration) {
        //this.htable = Hbase11xHelper.getTable(configuration);
        this.bufferedMutator = Hbase11xHelper.getBufferedMutator(configuration);
        this.columns = configuration.getListConfiguration(Key.COLUMN);
        this.rowkeyColumn = configuration.getListConfiguration(Key.ROWKEY_COLUMN);
        this.versionColumn = configuration.getConfiguration(Key.VERSION_COLUMN);
        this.encoding = configuration.getString(Key.ENCODING,Constant.DEFAULT_ENCODING);
        this.nullMode = NullModeType.getByTypeName(configuration.getString(Key.NULL_MODE,Constant.DEFAULT_NULL_MODE));
        this.walFlag = configuration.getBool(Key.WAL_FLAG, false);
    }

    public void startWriter(RecordReceiver lineReceiver, TaskPluginCollector taskPluginCollector){
        Record record;
        try {
            while ((record = lineReceiver.getFromReader()) != null) {
                Put put;
                try {
                    put = convertRecordToPut(record);
                } catch (Exception e) {
                    taskPluginCollector.collectDirtyRecord(record, e);
                    continue;
                }
                try {
                    //this.htable.put(put);
                    this.bufferedMutator.mutate(put);
                } catch (IllegalArgumentException e) {
                    if(e.getMessage().equals("No columns to insert") && nullMode.equals(NullModeType.Skip)){
                        LOG.info(String.format("record is empty, 您配置nullMode为[skip],将会忽略这条记录,record[%s]", record.toString()));
                        continue;
                    }else {
                        taskPluginCollector.collectDirtyRecord(record, e);
                        continue;
                    }
                }
            }
        }catch (IOException e){
            throw DataXException.asDataXException(Hbase11xWriterErrorCode.PUT_HBASE_ERROR,e);
        }finally {
            //Hbase11xHelper.closeTable(this.htable);
            Hbase11xHelper.closeBufferedMutator(this.bufferedMutator);
        }
    }


    public abstract  Put convertRecordToPut(Record record);

    public void close()  {
        //Hbase11xHelper.closeTable(this);
        Hbase11xHelper.closeBufferedMutator(this.bufferedMutator);
    }


    public byte[] getColumnByte(ColumnType columnType, Column column){
        byte[] bytes;
        if(column.getRawData() != null){
            switch (columnType) {
                case INT:
                    bytes = Bytes.toBytes(column.asLong().intValue());
                    break;
                case LONG:
                    bytes = Bytes.toBytes(column.asLong());
                    break;
                case DOUBLE:
                    bytes = Bytes.toBytes(column.asDouble());
                    break;
                case FLOAT:
                    bytes = Bytes.toBytes(column.asDouble().floatValue());
                    break;
                case SHORT:
                    bytes = Bytes.toBytes(column.asLong().shortValue());
                    break;
                case BOOLEAN:
                    bytes = Bytes.toBytes(column.asBoolean());
                    break;
                case STRING:
                    bytes = this.getValueByte(columnType,column.asString());
                    break;
                default:
                    throw DataXException.asDataXException(Hbase11xWriterErrorCode.ILLEGAL_VALUE, "HbaseWriter列不支持您配置的列类型:" + columnType);
            }
        }else{
            switch (nullMode){
                case Skip:
                    bytes =  null;
                    break;
                case Empty:
                    bytes = HConstants.EMPTY_BYTE_ARRAY;
                    break;
                default:
                    throw DataXException.asDataXException(Hbase11xWriterErrorCode.ILLEGAL_VALUE, "HbaseWriter nullMode不支持您配置的类型,只支持skip或者empty");
            }
        }
        return  bytes;
    }

    public byte[] getValueByte(ColumnType columnType, String value){
        byte[] bytes;
        if(value != null){
            switch (columnType) {
                case INT:
                    bytes = Bytes.toBytes(Integer.parseInt(value));
                    break;
                case LONG:
                    bytes = Bytes.toBytes(Long.parseLong(value));
                    break;
                case DOUBLE:
                    bytes = Bytes.toBytes(Double.parseDouble(value));
                    break;
                case FLOAT:
                    bytes = Bytes.toBytes(Float.parseFloat(value));
                    break;
                case SHORT:
                    bytes = Bytes.toBytes(Short.parseShort(value));
                    break;
                case BOOLEAN:
                    bytes = Bytes.toBytes(Boolean.parseBoolean(value));
                    break;
                case STRING:
                    bytes = value.getBytes(Charset.forName(encoding));
                    break;
                default:
                    throw DataXException.asDataXException(Hbase11xWriterErrorCode.ILLEGAL_VALUE, "HbaseWriter列不支持您配置的列类型:" + columnType);
            }
        }else{
            bytes = HConstants.EMPTY_BYTE_ARRAY;
        }
        return  bytes;
    }
}
