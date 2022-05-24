package com.alibaba.datax.plugin.writer.s3writer.writer;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.plugin.s3common.base.WriterBase;
import com.alibaba.datax.plugin.s3common.util.*;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.parquet.io.api.Binary;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author: duhanmin
 * Description:
 * Date: 2021/6/9 19:15
 */
public class Avro implements WriterBase<GenericData.Record> {

    private Configuration conf;
    private String path;
    private Path tmpFile;
    private String compression;
    private List<String> fullColumnNames;
    private List<String> fullColumnTypes;
    private ColumnTypeUtil.DecimalInfo PARQUET_DEFAULT_DECIMAL_INFO = new ColumnTypeUtil.DecimalInfo(10, 2);
    private transient Map<String, ColumnTypeUtil.DecimalInfo> decimalColInfo;
    private Schema schema;
    private DataFileWriter<GenericData.Record> dataFileWriter;

    public Avro(String path, Path tmpFile, String compression, List<String> fullColumnNames, List<String> fullColumnTypes,Configuration conf) {
        this.path = path;
        this.tmpFile = tmpFile;
        this.compression = compression;
        this.fullColumnNames = fullColumnNames;
        this.fullColumnTypes = fullColumnTypes;
        this.conf = conf;
        init();
    }

    @Override
    public void init() {
        try {
            buildSchemas();
            FileSystem fileSystem = FileSystem.get(tmpFile.toUri(), conf);
            FSDataOutputStream os = fileSystem.create(tmpFile);

            GenericDatumWriter<GenericData.Record> writer = new GenericDatumWriter<>(schema);
            this.dataFileWriter = new DataFileWriter<>(writer)
                    .setCodec(compressType(compression))
                    .create(schema, os);
        } catch (Exception e) {
            throw new RuntimeException("初始化错误",e);
        }
    }

    /**
     * 压缩
     * @return
     */
    @Override
    public CodecFactory compressType(String compression){
        // Compatible with old code
        if(StrUtil.isEmpty(compression)){
            compression = ECompressType.AVRO_NONE.getType();
        }

        ECompressType compressType = ECompressType.getByTypeAndFileType(compression, "avro");
        if(ECompressType.AVRO_SNAPPY.equals(compressType)){
            return CodecFactory.snappyCodec();
        }else if(ECompressType.AVRO_BZIP2.equals(compressType)){
            return CodecFactory.bzip2Codec();
        }else if(ECompressType.AVRO_DEFLATE.equals(compressType)){
            return CodecFactory.deflateCodec(-1);
        }else if(ECompressType.AVRO_XZ.equals(compressType)){
            return CodecFactory.xzCodec(6);
        } else {
            return CodecFactory.nullCodec();
        }
    }

    /**
     *
     * | ----------------------         | -----------|
     * | int/bigint/smallint/tinyint    | bigint   |
     * | decimal                        | decimal/double|
     * | double/float                   | double   |
     * | bit/binary/blob                | string   |
     * | char/varchar/text              | string   |
     * | time/datetime/timestamp/date   | bigint   |
     * | json                           | string   |
     * | boolean                        | boolean  |
     *
     * @return
     */
    @Override
    public void buildSchemas() {
        decimalColInfo = new HashMap<>(16);
        JSONArray fields = new JSONArray();
        for (int i = 0; i < fullColumnNames.size(); i++) {
            String name = fullColumnNames.get(i);
            String colType = fullColumnTypes.get(i).toLowerCase();
            JSONObject field = new JSONObject();
            Object type;
            JSONObject fieldType;
            switch (colType){
                case "tinyint" :
                case "smallint" :
                case "int" :
                case "bigint" :
                    type = Schema.Type.LONG.getName();break;
                case "timestamp" :
                    fieldType = new JSONObject(true);
                    fieldType.put("type",Schema.Type.LONG.getName());
                    fieldType.put("logicalType","timestamp-millis");
                    type = fieldType;
                    break;
                case "date" :
                    fieldType = new JSONObject(true);
                    fieldType.put("type",Schema.Type.INT.getName());
                    fieldType.put("logicalType","date");
                    type = fieldType;
                    break;
                case "float" :
                case "double" : type = Schema.Type.DOUBLE.getName();break;
                case "char" :
                case "varchar" :
                case "string" : type = Schema.Type.STRING.getName();break;
                case "boolean" : type = Schema.Type.BOOLEAN.getName();break;
                default:
                    if(ColumnTypeUtil.isDecimalType(colType)){
                        ColumnTypeUtil.DecimalInfo decimalInfo = ColumnTypeUtil.getDecimalInfo(colType, PARQUET_DEFAULT_DECIMAL_INFO);
                        fieldType = new JSONObject(true);
                        fieldType.put("type",Schema.Type.BYTES.getName());
                        fieldType.put("logicalType","decimal");
                        fieldType.put("precision",decimalInfo.getPrecision());
                        fieldType.put("scale",decimalInfo.getScale());
                        type = fieldType;
                        decimalColInfo.put(name, decimalInfo);
                    } else {
                        type = Schema.Type.STRING.getName();break;
                    }
                    break;
            }
            field.put("name",name);
            field.put("type",type);
            fields.add(field);
        }

        JSONObject json = new JSONObject(true);
        String[] dbTable = StrUtil.split(path, "/");
        json.put("namespace",dbTable[0]);
        json.put("type","record");
        json.put("name",dbTable[1]);
        json.put("fields",fields);
        this.schema = new Schema.Parser().parse(json.toJSONString());
    }

    /**
     * 写入GenericData入文件
     * @param lineReceiver
     */
    @Override
    public void writer(RecordReceiver lineReceiver) throws IOException {
        Record record;
        while ((record = lineReceiver.getFromReader()) != null) {
            GenericData.Record genericDataRecord = new GenericData.Record(schema);
            for (int i = 0; i < fullColumnNames.size(); i++) {
                Column column = record.getColumn(i);
                addData(genericDataRecord,column,i);
            }
            dataFileWriter.append(genericDataRecord);
        }
    }

    /**
     * 构造GenericData
     * @param record
     * @param column
     * @param i
     */
    @Override
    public void addData(GenericData.Record record, Column column, int i){
        String colName = fullColumnNames.get(i);
        Object valObj = column.getRawData();
        if (valObj == null) {
            record.put(colName,"");
            return;
        }

        String colType = fullColumnTypes.get(i);
        colType = ColumnType.fromString(colType).name().toLowerCase();
        String val = valObj.toString();

        switch (colType){
            case "tinyint" :
            case "smallint" :
            case "int" :
            case "bigint" :
                if (valObj instanceof Timestamp){
                    record.put(colName,((Timestamp) valObj).getTime());
                } else if(valObj instanceof Date){
                    record.put(colName,((Date) valObj).getTime());
                } else {
                    record.put(colName,Long.parseLong(val));
                }
                break;
            case "float" :
            case "double" : record.put(colName,Double.parseDouble(val));break;
            case "binary" :record.put(colName, Binary.fromString(val));break;
            case "char" :
            case "varchar" :
            case "string" :
                if (valObj instanceof Timestamp){
                    val= DateUtil.getDateTimeFormatterForMillisencond().format(valObj);
                    record.put(colName,val);
                }else if (valObj instanceof Map || JSON.toJSON(valObj) instanceof Map || valObj instanceof List || JSON.toJSON(valObj) instanceof List){
                    record.put(colName, JSON.toJSONString(valObj));
                }else if (valObj instanceof byte[]){
                    record.put(colName, new String((byte[])valObj));
                }else {
                    record.put(colName,val);
                }
                break;
            case "boolean" :
                record.put(colName,column.asBoolean());break;
            case "timestamp" :
                Timestamp ts = DateUtil.columnToTimestamp(valObj,null);
                record.put(colName, ts.getTime());
                break;
            case "decimal" :
                ColumnTypeUtil.DecimalInfo decimalInfo = decimalColInfo.get(colName);
                HiveDecimal hiveDecimal = HiveDecimal.create(new BigDecimal(val));
                hiveDecimal = HiveDecimal.enforcePrecisionScale(hiveDecimal, decimalInfo.getPrecision(), decimalInfo.getScale());
                if(ObjectUtil.isNull(hiveDecimal)){
                    String msg = String.format("第[%s]个数据数据[%s]precision和scale和元数据不匹配:decimal(%s, %s)", i, decimalInfo.getPrecision(), decimalInfo.getScale(), valObj);
                    throw new RuntimeException(msg, new IllegalArgumentException());
                }
                record.put(colName, HdfsUtil.getBufferFromDecimal(hiveDecimal,decimalInfo.getScale()));
                break;
            case "date" :
                Date date = DateUtil.columnToDate(valObj,null);
                record.put(colName, DateWritable.dateToDays(new java.sql.Date(date.getTime())));
                break;
            default:
                try {
                    record.put(colName, JSON.toJSONString(valObj));
                }catch (Exception e){
                    record.put(colName,val);
                }
                break;
        }
    }

    @Override
    public void close() throws IOException {
        IoUtil.close(dataFileWriter);
    }
}