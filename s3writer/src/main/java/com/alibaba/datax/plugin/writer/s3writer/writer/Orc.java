package com.alibaba.datax.plugin.writer.s3writer.writer;

import cn.hutool.core.util.ObjectUtil;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.plugin.s3common.base.WriterBase;
import com.alibaba.datax.plugin.s3common.util.*;
import com.alibaba.fastjson.JSON;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Author: duhanmin
 * Description:
 * Date: 2021/6/8 11:38
 */
public class Orc implements WriterBase<List<Object>> {

    private Configuration conf;
    private Path path;
    private String compression;
    private List<String> fullColumnNames;
    private List<String> fullColumnTypes;
    private StructObjectInspector inspector;
    private RecordWriter recordWriter;
    private OrcSerde orcSerde;
    protected transient Map<String, ColumnTypeUtil.DecimalInfo> decimalColInfo;
    private static ColumnTypeUtil.DecimalInfo ORC_DEFAULT_DECIMAL_INFO = new ColumnTypeUtil.DecimalInfo(HiveDecimal.SYSTEM_DEFAULT_PRECISION, HiveDecimal.SYSTEM_DEFAULT_SCALE);

    public Orc(Path tmpFile, String compression, List<String> fullColumnNames, List<String> fullColumnTypes,Configuration conf) {
        this.path = tmpFile;
        this.compression = compression;
        this.fullColumnNames = fullColumnNames;
        this.fullColumnTypes = fullColumnTypes;
        this.conf = conf;
        init();
    }

    @Override
    public void init() {
        try {
            this.orcSerde = new OrcSerde();
            FileOutputFormat outputFormat = new OrcOutputFormat();
            JobConf jobConf = new JobConf(this.conf);
            FileOutputFormat.setOutputCompressorClass(jobConf, compressType(compression));

            List<ObjectInspector>  fullColTypeList = new ArrayList<>();

            decimalColInfo = new HashMap<>((fullColumnTypes.size()<<2)/3);
            for (int i = 0; i < fullColumnTypes.size(); i++) {
                String columnType = fullColumnTypes.get(i);

                if(ColumnTypeUtil.isDecimalType(columnType)) {
                    ColumnTypeUtil.DecimalInfo decimalInfo = ColumnTypeUtil.getDecimalInfo(columnType, ORC_DEFAULT_DECIMAL_INFO);
                    decimalColInfo.put(fullColumnNames.get(i), decimalInfo);
                }

                ColumnType type = ColumnType.getType(columnType);
                fullColTypeList.add(HdfsUtil.columnTypeToObjectInspetor(type));
            }

            this.inspector = ObjectInspectorFactory.getStandardStructObjectInspector(fullColumnNames, fullColTypeList);
            recordWriter = outputFormat.getRecordWriter(null, jobConf, path.toUri().toString(), Reporter.NULL);
        } catch (IOException e) {
            throw new RuntimeException("Orc初始化失败",e);
        }
    }

    @Override
    public Class compressType(String compression) {
        ECompressType compressType = ECompressType.getByTypeAndFileType(compression, "orc");
        if(ECompressType.ORC_SNAPPY.equals(compressType)){
            return SnappyCodec.class;
        } else if(ECompressType.ORC_BZIP.equals(compressType)){
            return BZip2Codec.class;
        } else if(ECompressType.ORC_GZIP.equals(compressType)){
            return GzipCodec.class;
        } else if(ECompressType.ORC_LZ4.equals(compressType)){
            return Lz4Codec.class;
        } else {
            return DefaultCodec.class;
        }
    }

    @Override
    public void writer(RecordReceiver lineReceiver) throws IOException {
        Record record;
        while ((record = lineReceiver.getFromReader()) != null) {
            List<Object> recordList = new ArrayList<>();
            for (int i = 0; i < fullColumnNames.size(); i++) {
                Column column = record.getColumn(i);
                addData(recordList,column,i);
            }
            this.recordWriter.write(NullWritable.get(), this.orcSerde.serialize(recordList, this.inspector));
        }
    }

    /**
     * 构造数据
     * @param recordList
     * @param column
     * @param i
     */
    public void addData(List<Object> recordList, Column column, int i) {

        Object rowDataObj = column.getRawData();
        if (ObjectUtil.isNull(rowDataObj)) {
            recordList.add(null);
            return;
        }

        ColumnType columnType = ColumnType.fromString(fullColumnTypes.get(i));
        String rowData = rowDataObj.toString();
        if(ObjectUtil.isNull(rowData) || (rowData.length() == 0 && !ColumnType.isStringType(columnType)) ){
            recordList.add(null);
            return;
        }

        switch (columnType) {
            case TINYINT:
                recordList.add(Byte.valueOf(rowData));
            case SMALLINT:
            case INT:
            case BIGINT:
                if (rowDataObj instanceof Timestamp){
                    recordList.add(((Timestamp) rowDataObj).getTime());break;
                } else if(rowDataObj instanceof Date){
                    recordList.add(((Date) rowDataObj).getTime());break;
                }
                BigInteger data = new BigInteger(rowData);
                if (data.compareTo(new BigInteger(String.valueOf(Long.MAX_VALUE))) > 0){
                    recordList.add(data);
                } else {
                    recordList.add(Long.valueOf(rowData));
                }
                break;
            case FLOAT:
            case DOUBLE:
                recordList.add(Double.valueOf(rowData));
                break;
            case DECIMAL:
                ColumnTypeUtil.DecimalInfo decimalInfo = decimalColInfo.get(fullColumnNames.get(i));
                recordList.add(HdfsUtil.getDecimalWritable(rowData,decimalInfo));
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
                if (rowDataObj instanceof Timestamp){
                    SimpleDateFormat fm = DateUtil.getDateTimeFormatterForMillisencond();
                    recordList.add(fm.format(rowDataObj));
                }else if (rowDataObj instanceof Map || JSON.toJSON(rowDataObj) instanceof Map || rowDataObj instanceof List || JSON.toJSON(rowDataObj) instanceof List){
                    recordList.add(JSON.toJSONString(rowDataObj));
                }else if (rowDataObj instanceof byte[]){
                    recordList.add(new String((byte[])rowDataObj));
                }else {
                    recordList.add(rowData);
                }
                break;
            case BOOLEAN:
                recordList.add(Boolean.valueOf(rowData));
                break;
            case DATE:
                java.sql.Date date = DateUtil.columnToDate(rowDataObj, null);
                org.apache.hadoop.hive.common.type.Date dateHive = new org.apache.hadoop.hive.common.type.Date();
                dateHive.setTimeInMillis(date.getTime());
                recordList.add(new DateWritableV2(dateHive));
                break;
            case TIMESTAMP:
                Timestamp timestamp = DateUtil.columnToTimestamp(rowDataObj, null);
                org.apache.hadoop.hive.common.type.Timestamp timestampHive = new org.apache.hadoop.hive.common.type.Timestamp();
                timestampHive.setTimeInMillis(timestamp.getTime(),timestamp.getNanos());
                recordList.add(new TimestampWritableV2(timestampHive));
                break;
            case BINARY:
                recordList.add(new BytesWritable(rowData.getBytes(StandardCharsets.UTF_8)));
                break;
            default:
                recordList.add(rowData);
                break;
        }
    }

    @Override
    public void close() throws IOException {
        if(recordWriter != null) {
            recordWriter.close(Reporter.NULL);
            this.recordWriter = null;
        }

    }

    @Override
    public void buildSchemas() {
    }

}
