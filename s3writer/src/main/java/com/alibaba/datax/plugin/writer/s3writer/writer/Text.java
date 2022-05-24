package com.alibaba.datax.plugin.writer.s3writer.writer;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.ObjectUtil;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.plugin.s3common.base.WriterBase;
import com.alibaba.datax.plugin.s3common.util.*;
import com.alibaba.fastjson.JSON;
import io.airlift.compress.lz4.Lz4Codec;
import io.airlift.compress.snappy.SnappyCodec;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Author: duhanmin
 * Description:
 * Date: 2021/6/8 10:18
 */
@Slf4j
public class Text implements WriterBase<List<Object>> {

    private Configuration conf;
    private String fieldDelimiter;
    private List<String> fullColumnTypes;
    private Path path;
    private String compression;
    private transient RecordWriter stream;
    private FileSystem fs;
    private ColumnTypeUtil.DecimalInfo PARQUET_DEFAULT_DECIMAL_INFO = new ColumnTypeUtil.DecimalInfo(10, 2);

    public Text(String fieldDelimiter,List<String> fullColumnTypes,Path tmpFile,String compression,Configuration conf) {
        this.fieldDelimiter = fieldDelimiter;
        this.fullColumnTypes = fullColumnTypes;
        this.path = tmpFile;
        this.compression = compression;
        this.conf = conf;
        init();
    }

    @Override
    public void init() {
        try {
            this.fs = FileSystemUtil.getFileSystem(null, null);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
            String attempt = "attempt_"+dateFormat.format(new Date())+"_0001_m_000000_0";
            JobConf conf = new JobConf(this.conf);
            conf.set(JobContext.TASK_ATTEMPT_ID, attempt);
            FileOutputFormat outFormat = new TextOutputFormat();
            Class<? extends CompressionCodec> compressionCodec = compressType(compression);
            if (compressionCodec != null)
                FileOutputFormat.setOutputCompressorClass(conf, compressionCodec);
            FileOutputFormat.setOutputPath(conf,path);
            stream = outFormat.getRecordWriter(fs, conf, path.toString(), Reporter.NULL);
        } catch (Exception e) {
            throw new RuntimeException("初始化错误",e);
        }
    }

    @Override
    public Class<? extends CompressionCodec> compressType(String compress) throws Exception {
        Class<? extends CompressionCodec> codecClass = null;
        ECompressType compressType = ECompressType.getByTypeAndFileType(compress, "text");
        if(ECompressType.TEXT_NONE.equals(compressType)){
        } else if (ECompressType.TEXT_GZIP.equals(compressType)){
            codecClass = GzipCodec.class;
        } else if(ECompressType.TEXT_BZIP2.equals(compressType)){
            codecClass = BZip2Codec.class;
        }  else if(ECompressType.TEXT_LZ4.equals(compressType)){
            codecClass = Lz4Codec.class;
        } else if (ECompressType.TEXT_SNAPPY.equals(compressType)) {
            codecClass = SnappyCodec.class;
        }
        return codecClass;
    }

    @Override
    public void buildSchemas() {
    }

    /**
     * 写text
     * @param lineReceiver
     * @return
     */
    @Override
    public void writer(RecordReceiver lineReceiver) throws IOException {
        Record record;
        while ((record = lineReceiver.getFromReader()) != null) {
            List<Object> list = new ArrayList<>();
            for (int i = 0; i < fullColumnTypes.size(); i++) {
                Column valObj = record.getColumn(i);
                addData(list,valObj,i);
            }

            if (list.size() != 0){
                stream.write(NullWritable.get(),CollUtil.join(list,fieldDelimiter));
            }
        }
    }

    /**
     * 类型匹配
     *
     * @param list
     * @param column
     * @param i
     */
    @Override
    public void addData(List<Object> list, Column column, int i) {
        if(ObjectUtil.isNull(column)) {
            list.add(HdfsUtil.NULL_VALUE);
        }else {
            Object rowDataObj = column.getRawData();
            if(ObjectUtil.isEmpty(rowDataObj)) {
                list.add(HdfsUtil.NULL_VALUE);
            }else {
                String rowData = rowDataObj.toString();
                ColumnType columnType = ColumnType.fromString(fullColumnTypes.get(i));
                switch (columnType) {
                    case TINYINT:
                    case SMALLINT:
                    case INT:
                    case BIGINT:
                        if (rowDataObj instanceof Timestamp){
                            list.add(((Timestamp) rowDataObj).getTime());break;
                        } else if(rowDataObj instanceof Date){
                            list.add(((Date) rowDataObj).getTime());break;
                        }
                        BigInteger data = new BigInteger(rowData);
                        if (data.compareTo(new BigInteger(String.valueOf(Long.MAX_VALUE))) > 0){
                            list.add(data);
                        } else {
                            list.add(Long.valueOf(rowData));
                        }
                        break;
                    case FLOAT:
                    case DOUBLE:
                        list.add(Double.valueOf(rowData));
                        break;
                    case DECIMAL:
                        ColumnTypeUtil.DecimalInfo decimalInfo = ColumnTypeUtil.getDecimalInfo(fullColumnTypes.get(i), PARQUET_DEFAULT_DECIMAL_INFO);
                        HiveDecimal hiveDecimal = HiveDecimal.create(new BigDecimal(rowData));
                        hiveDecimal = HiveDecimal.enforcePrecisionScale(hiveDecimal, decimalInfo.getPrecision(), decimalInfo.getScale());
                        if(hiveDecimal == null){
                            String msg = String.format("第[%s]个数据数据[%s]precision和scale和元数据不匹配:decimal(%s, %s)", i, decimalInfo.getPrecision(), decimalInfo.getScale(), rowData);
                            throw new RuntimeException(msg, new IllegalArgumentException());
                        }
                        list.add(hiveDecimal);
                        break;
                    case STRING:
                    case VARCHAR:
                    case CHAR:
                        if (rowDataObj instanceof Timestamp){
                            SimpleDateFormat fm = DateUtil.getDateTimeFormatterForMillisencond();
                            list.add(fm.format((Timestamp)rowDataObj));
                        }else if (rowDataObj instanceof Map || JSON.toJSON(rowDataObj) instanceof Map || rowDataObj instanceof List || JSON.toJSON(rowDataObj) instanceof List){
                            list.add(JSON.toJSONString(rowDataObj));
                        }else if (rowDataObj instanceof byte[]){
                            list.add(new String((byte[])rowDataObj));
                        }else {
                            list.add(rowDataObj);
                        }
                        break;
                    case BOOLEAN:
                        list.add(Boolean.valueOf(rowData));
                        break;
                    case DATE:
                        rowDataObj = DateUtil.columnToDate(rowDataObj,null);
                        list.add(DateUtil.dateToString((Date) rowDataObj));
                        break;
                    case TIMESTAMP:
                        rowDataObj = DateUtil.columnToTimestamp(rowDataObj,null);
                        list.add(DateUtil.timestampToString((Date)rowDataObj));
                        break;
                    default:
                        list.add(rowData);
                        break;
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (stream != null){
            stream.close(Reporter.NULL);
        }
        IoUtil.close(fs);
    }
}
