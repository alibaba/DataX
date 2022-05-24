package com.alibaba.datax.plugin.reader.s3reader.reader;

import cn.hutool.core.io.IoUtil;
import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.plugin.s3common.base.ReaderBase;
import com.alibaba.datax.plugin.s3common.util.AWSExecutor;
import com.alibaba.datax.plugin.s3common.util.ColumnType;
import com.alibaba.datax.plugin.s3common.util.DateUtil;
import com.alibaba.datax.plugin.s3common.util.DecimalUtil;
import com.alibaba.datax.plugin.s3common.entry.ColumnEntry;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * Author: duhanmin
 * Description:
 * Date: 2021/7/7 18:31
 */
@Slf4j
public class Parquet implements ReaderBase<Group> {

    private List<ColumnEntry> columns;
    private AWSExecutor s3Client;
    private ParquetReader<Group> groups;
    public Parquet(List<ColumnEntry> columns, AWSExecutor s3Client) {
        this.columns = columns;
        this.s3Client = s3Client;
    }

    @Override
    public void reader(String sourceFile, RecordSender recordSender) {
        try {
            groups = ParquetReader.builder(new GroupReadSupport(), s3Client.path(sourceFile))
                    .withConf(s3Client.getConf())
                    .build();

            Group group;
            while((group = groups.read()) != null){
                Record record = recordSender.createRecord();
                for (ColumnEntry columnConfig : columns) {
                    Column column = addData(group,columnConfig.getType(),columnConfig.getIndex(),columnConfig.getFormat());
                    record.addColumn(column);
                }
                recordSender.sendToWriter(record);
            }

            close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        IoUtil.close(groups);
    }

    @Override
    public Column addData(Group currentLine, String type, int index, String format){
        ColumnType columnType = ColumnType.fromString(type);

        Column columnGenerated;
        SimpleDateFormat customTimeFormat = DateUtil.getSimpleDateFormat(format);
        try{
            if (index == -1 || currentLine == null){
                columnGenerated = new StringColumn(null);
            }

            Type colSchemaType = currentLine.getType().getType(index);
            switch (columnType.name().toLowerCase()){
                case "tinyint" :
                case "smallint" :
                case "int" :
                case "bigint" :
                    columnGenerated = new LongColumn(currentLine.getLong(index,0));
                    break;
                case "float" :
                case "double" :
                    columnGenerated = new DoubleColumn(currentLine.getDouble(index,0));
                    break;
                case "binary" :
                    columnGenerated = new BytesColumn(currentLine.getBinary(index,0).getBytes());
                    break;
                case "char" :
                case "varchar" :
                case "string" :
                    String str = currentLine.getString(index, 0);
                    if(customTimeFormat != null){
                        str = DateUtil.timestampToString(DateUtil.columnToDate(str,customTimeFormat));
                    }
                    columnGenerated = new StringColumn(str);
                    break;
                case "boolean" :
                    columnGenerated = new BoolColumn(currentLine.getBoolean(index,0));
                    break;
                case "timestamp" :
                    long time = DateUtil.getTimestampMillis(currentLine.getInt96(index,0));
                    columnGenerated = new DateColumn(new Timestamp(time));
                    break;
                case "decimal" :
                    BigDecimal data;
                    DecimalMetadata dm = ((PrimitiveType) colSchemaType).getDecimalMetadata();
                    String primitiveTypeName = currentLine.getType().getType(index).asPrimitiveType().getPrimitiveTypeName().name();
                    if (ColumnType.INT32.name().equals(primitiveTypeName)){
                        int intVal = currentLine.getInteger(index,0);
                        data = DecimalUtil.longToDecimal(intVal,dm.getScale());
                    } else if(ColumnType.INT64.name().equals(primitiveTypeName)){
                        long longVal = currentLine.getLong(index,0);
                        data = DecimalUtil.longToDecimal(longVal,dm.getScale());
                    } else {
                        Binary binary = currentLine.getBinary(index,0);
                        data = DecimalUtil.binaryToDecimal(binary,dm.getScale());
                    }
                    columnGenerated = new DoubleColumn(data);
                    break;
                case "date" :
                    String val = currentLine.getValueToString(index,0);
                    columnGenerated = new DateColumn(new Date(new Timestamp(Integer.parseInt(val) * DateUtil.MILLIS_IN_DAY).getTime()));
                    break;
                default:
                    columnGenerated = new StringColumn(currentLine.getValueToString(index,0));
                    break;
            }
        } catch (Exception e){
            columnGenerated = new StringColumn(null);
        }

        return columnGenerated;
    }
}
