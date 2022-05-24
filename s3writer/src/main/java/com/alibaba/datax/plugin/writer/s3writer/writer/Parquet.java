package com.alibaba.datax.plugin.writer.s3writer.writer;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.plugin.s3common.base.WriterBase;
import com.alibaba.datax.plugin.s3common.util.*;
import com.alibaba.fastjson.JSON;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

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
 * Date: 2021/6/8 09:58
 */
public class Parquet implements WriterBase<Group> {

    private Configuration conf;
    private Path path;
    private String compression;
    private List<String> fullColumnNames;
    private List<String> fullColumnTypes;
    private SimpleGroupFactory groupFactory;
    private ParquetWriter<Group> writer;
    private MessageType schema;
    private ColumnTypeUtil.DecimalInfo PARQUET_DEFAULT_DECIMAL_INFO = new ColumnTypeUtil.DecimalInfo(10, 2);
    private transient Map<String, ColumnTypeUtil.DecimalInfo> decimalColInfo;

    public Parquet(Path tmpFile, String compression, List<String> fullColumnNames, List<String> fullColumnTypes,Configuration conf) {
        this.path = tmpFile;
        this.compression = compression;
        this.fullColumnNames = fullColumnNames;
        this.fullColumnTypes = fullColumnTypes;
        this.conf = conf;
        buildSchemas();
        init();
    }

    /**
     * 写入ParquetWriter构造器
     * @return
     */
    @Override
    public void init() {
        this.groupFactory = new SimpleGroupFactory(schema);
        try {
            this.writer = ExampleParquetWriter
                    .builder(path)
                    .withWriteMode(ParquetFileWriter.Mode.CREATE)
                    .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                    .withCompressionCodec(compressType(compression))
                    .withConf(this.conf)
                    .withType(schema)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException("Parquet初始化失败",e);
        }
    }

    /**
     * 压缩
     * @return
     */
    @Override
    public CompressionCodecName compressType(String compression){
        // Compatible with old code
        if(StrUtil.isEmpty(compression)){
            compression = ECompressType.PARQUET_NONE.getType();
        }

        ECompressType compressType = ECompressType.getByTypeAndFileType(compression, "parquet");
        if(ECompressType.PARQUET_SNAPPY.equals(compressType)){
            return CompressionCodecName.SNAPPY;
        }else if(ECompressType.PARQUET_GZIP.equals(compressType)){
            return CompressionCodecName.GZIP;
        }else if(ECompressType.PARQUET_LZO.equals(compressType)){
            return CompressionCodecName.LZO;
        }else if(ECompressType.PARQUET_LZ4.equals(compressType)){
            return CompressionCodecName.LZ4;
        } else {
            return CompressionCodecName.UNCOMPRESSED;
        }
    }

    /**
     * 构造Schemas
     * @return
     */
    @Override
    public void buildSchemas(){
        decimalColInfo = new HashMap<>(16);
        Types.MessageTypeBuilder typeBuilder = Types.buildMessage();
        for (int i = 0; i < fullColumnNames.size(); i++) {
            String name = fullColumnNames.get(i);
            String colType = fullColumnTypes.get(i).toLowerCase();
            switch (colType){
                case "tinyint" :
                case "smallint" :
                case "int" : typeBuilder.optional(PrimitiveType.PrimitiveTypeName.INT32).named(name);break;
                case "bigint" : typeBuilder.optional(PrimitiveType.PrimitiveTypeName.INT64).named(name);break;
                case "float" : typeBuilder.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named(name);break;
                case "double" : typeBuilder.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named(name);break;
                case "binary" :typeBuilder.optional(PrimitiveType.PrimitiveTypeName.BINARY).named(name);break;
                case "char" :
                case "varchar" :
                case "string" : typeBuilder.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(name);break;
                case "boolean" : typeBuilder.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(name);break;
                case "timestamp" : typeBuilder.optional(PrimitiveType.PrimitiveTypeName.INT96).named(name);break;
                case "date" :typeBuilder.optional(PrimitiveType.PrimitiveTypeName.INT32).as(OriginalType.DATE).named(name);break;
                default:
                    if(ColumnTypeUtil.isDecimalType(colType)){
                        ColumnTypeUtil.DecimalInfo decimalInfo = ColumnTypeUtil.getDecimalInfo(colType, PARQUET_DEFAULT_DECIMAL_INFO);
                        typeBuilder.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                                .as(OriginalType.DECIMAL)
                                .precision(decimalInfo.getPrecision())
                                .scale(decimalInfo.getScale())
                                .length(HdfsUtil.computeMinBytesForPrecision(decimalInfo.getPrecision()))
                                .named(name);

                        decimalColInfo.put(name, decimalInfo);
                    } else {
                        typeBuilder.optional(PrimitiveType.PrimitiveTypeName.BINARY).named(name);
                    }
                    break;
            }
        }
        this.schema = typeBuilder.named("Pair");
    }

    /**
     * 写入Group入文件
     * @param lineReceiver
     * @throws IOException
     */
    @Override
    public void writer(RecordReceiver lineReceiver) throws IOException {
        Record record;
        while ((record = lineReceiver.getFromReader()) != null) {
            Group group = groupFactory.newGroup();
            for (int i = 0; i < fullColumnNames.size(); i++) {
                Column column = record.getColumn(i);
                addData(group,column,i);
            }
            writer.write(group);
        }
    }

    /**
     * 构造group
     * @param group
     * @param column
     * @param i
     */
    @Override
    public void addData(Group group, Column column, int i){
        String colName = fullColumnNames.get(i);
        String colType = fullColumnTypes.get(i);
        colType = ColumnType.fromString(colType).name().toLowerCase();

        Object valObj = column.getRawData();
        String val = (valObj == null) ? null : valObj.toString();
        if (valObj == null) return;
        switch (colType){
            case "tinyint" :
            case "smallint" :
            case "int" :
            case "bigint" :
                if (valObj instanceof Timestamp){
                    group.add(colName,((Timestamp) valObj).getTime());
                } else if(valObj instanceof Date){
                    group.add(colName,((Date) valObj).getTime());
                } else {
                    group.add(colName,Long.parseLong(val));
                }
                break;
            case "float" :
            case "double" : group.add(colName,Double.parseDouble(val));break;
            case "binary" :group.add(colName,Binary.fromString(val));break;
            case "char" :
            case "varchar" :
            case "string" :
                if (valObj instanceof Timestamp){
                    val= DateUtil.getDateTimeFormatterForMillisencond().format(valObj);
                    group.add(colName,val);
                }else if (valObj instanceof Map || JSON.toJSON(valObj) instanceof Map || valObj instanceof List || JSON.toJSON(valObj) instanceof List){
                    group.add(colName, JSON.toJSONString(valObj));
                }else if (valObj instanceof byte[]){
                    group.add(colName, new String((byte[])valObj));
                }else {
                    group.add(colName,val);
                }
                break;
            case "boolean" :
                group.add(colName,column.asBoolean());break;
            case "timestamp" :
                Timestamp ts = DateUtil.columnToTimestamp(valObj,null);
                byte[] dst = HdfsUtil.longToByteArray(ts.getTime());
                group.add(colName, Binary.fromConstantByteArray(dst));
                break;
            case "decimal" :
                ColumnTypeUtil.DecimalInfo decimalInfo = decimalColInfo.get(colName);

                HiveDecimal hiveDecimal = HiveDecimal.create(new BigDecimal(val));
                hiveDecimal = HiveDecimal.enforcePrecisionScale(hiveDecimal, decimalInfo.getPrecision(), decimalInfo.getScale());
                if(hiveDecimal == null){
                    String msg = String.format("第[%s]个数据数据[%s]precision和scale和元数据不匹配:decimal(%s, %s)", i, decimalInfo.getPrecision(), decimalInfo.getScale(), valObj);
                    throw new RuntimeException(msg, new IllegalArgumentException());
                }
                group.add(colName, HdfsUtil.decimalToBinary(hiveDecimal, decimalInfo.getPrecision(), decimalInfo.getScale()));
                //group.add(colName,Double.parseDouble(val));
                break;
            case "date" :
                Date date = DateUtil.columnToDate(valObj,null);
                group.add(colName, DateWritable.dateToDays(new java.sql.Date(date.getTime())));
                break;
            default:
                try {
                    group.add(colName, JSON.toJSONString(valObj));
                }catch (Exception e){
                    group.add(colName,val);
                }
                break;
        }
    }

    @Override
    public void close() throws IOException {
        IoUtil.close(writer);
    }
}
