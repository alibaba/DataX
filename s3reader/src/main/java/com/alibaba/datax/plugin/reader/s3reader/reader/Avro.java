package com.alibaba.datax.plugin.reader.s3reader.reader;

import cn.hutool.core.io.IoUtil;
import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.plugin.s3common.base.ReaderBase;
import com.alibaba.datax.plugin.s3common.util.AWSExecutor;
import com.alibaba.datax.plugin.s3common.util.ColumnType;
import com.alibaba.datax.plugin.s3common.util.ColumnTypeUtil;
import com.alibaba.datax.plugin.s3common.util.DateUtil;
import com.alibaba.datax.plugin.s3common.entry.ColumnEntry;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * Author: duhanmin
 * Description:
 * Date: 2021/7/7 18:32
 */
public class Avro implements ReaderBase {

    private List<ColumnEntry> columns;
    private AWSExecutor s3Client;
    private DataFileStream<GenericRecord> reader;
    private FileSystem fileSystem;
    private FSDataInputStream os;
    private ColumnTypeUtil.DecimalInfo PARQUET_DEFAULT_DECIMAL_INFO = new ColumnTypeUtil.DecimalInfo(10, 2);

    public Avro(List<ColumnEntry> columns, AWSExecutor s3Client) {
        this.columns = columns;
        this.s3Client = s3Client;
    }

    @Override
    public void reader(String sourceFile, RecordSender recordSender) {
        try {
            Path path = s3Client.path(sourceFile);
            fileSystem = FileSystem.get(path.toUri(), s3Client.getConf());
            os = fileSystem.open(path);
            reader = new DataFileStream<>(os,new GenericDatumReader<>());
            List<Schema.Field> schemas = reader.getSchema().getFields();
            List<GenericRecord> genericRecords = IteratorUtils.toList(reader);

            if (columns.size() != schemas.size())
                throw new RuntimeException("columns and schemas Inconsistent.");

            for (GenericRecord genericRecord:genericRecords) {
                Record record = recordSender.createRecord();
                for (int i = 0; i < schemas.size(); i++) {
                    ColumnEntry columnEntry = columns.get(i);
                    Schema.Field field = schemas.get(i);
                    String type = columnEntry.getType();
                    Object obj = genericRecord.get(field.name());
                    Column column = addData(obj,type,columnEntry.getIndex(),columnEntry.getFormat());
                    record.addColumn(column);
                }
                recordSender.sendToWriter(record);
            }
            
            genericRecords.clear();
            close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        IoUtil.close(reader);
        IoUtil.close(fileSystem);
        IoUtil.close(os);
    }

    @Override
    public Column addData(Object obj, String type, int index, String format) {
        if(obj == null || type == null){
            return new StringColumn(null);
        }
        String str = obj.toString();
        if(str.length() == 0){
            return new StringColumn(null);
        }

        Column columnGenerated;
        SimpleDateFormat customTimeFormat = DateUtil.getSimpleDateFormat(format);

        ColumnType columnType = ColumnType.getType(type.toUpperCase());
        switch(columnType) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case MEDIUMINT:
            case BIGINT:
                columnGenerated = new LongColumn(Long.valueOf(str.trim()));
                break;
            case FLOAT:
            case DOUBLE:
                columnGenerated = new DoubleColumn(Double.valueOf(str.trim()));
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
                if(customTimeFormat != null){
                    str = DateUtil.timestampToString(DateUtil.columnToDate(str,customTimeFormat));
                }
                columnGenerated = new StringColumn(str);
                break;
            case DECIMAL:
                ColumnTypeUtil.DecimalInfo decimalInfo = ColumnTypeUtil.getDecimalInfo(type, PARQUET_DEFAULT_DECIMAL_INFO);
                HiveDecimal dec = AvroSerdeUtils.getHiveDecimalFromByteBuffer((ByteBuffer) obj, decimalInfo.getScale());
                columnGenerated = new DoubleColumn(dec.bigDecimalValue());
                break;
            case BOOLEAN:
                columnGenerated = new BoolColumn(Boolean.valueOf(str.trim().toLowerCase()));
                break;
            case DATE:
                Date date = Date.ofEpochMilli(DateWritableV2.daysToMillis((Integer) obj));
                columnGenerated = new DateColumn(DateUtil.columnToDate(date.toEpochDay(),null));
                break;
            case TIMESTAMP:
                Timestamp timestamp = Timestamp.ofEpochMilli((Long)obj);
                columnGenerated = new DateColumn(timestamp.toSqlTimestamp());
                break;
            default:
                columnGenerated = new StringColumn(str);
        }
        return columnGenerated;
    }
}
