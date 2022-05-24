package com.alibaba.datax.plugin.reader.s3reader.reader;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.plugin.s3common.base.ReaderBase;
import com.alibaba.datax.plugin.s3common.entry.ColumnEntry;
import com.alibaba.datax.plugin.s3common.util.AWSExecutor;
import com.alibaba.datax.plugin.s3common.util.ColumnType;
import com.alibaba.datax.plugin.s3common.util.DateUtil;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Properties;

/**
 * Author: duhanmin
 * Description:
 * Date: 2021/7/7 17:35
 */
@Slf4j
public class Orc implements ReaderBase {

    private List<ColumnEntry> columns;
    private AWSExecutor s3Client;
    private Properties p = new Properties();
    private RecordReader reader;

    public Orc(List<ColumnEntry> columns, AWSExecutor s3Client) {
        this.columns = columns;
        this.s3Client = s3Client;
        init();
    }

    private void init() {
        int columnIndexMax = -1;
        for (ColumnEntry columnConfig : columns) {
            Integer columnIndex = columnConfig.getIndex();
            if (columnIndex != null && columnIndex < 0) {
                String message = String.format("您column中配置的index不能小于0，请修改为正确的index,column配置:%s", JSON.toJSONString(columns));
                throw new RuntimeException(message);
            } else if (columnIndex != null && columnIndex > columnIndexMax) {
                columnIndexMax = columnIndex;
            }
        }

        StringBuilder allColumns = new StringBuilder();
        StringBuilder allColumnTypes = new StringBuilder();
        for (ColumnEntry column:columns) {
            allColumns.append("col");
            allColumnTypes.append(column.getType());
            allColumns.append(",");
            allColumnTypes.append(":");
        }

        p.setProperty("columns", allColumns.toString());
        p.setProperty("columns.types", allColumnTypes.toString());
    }

    @Override
    public void reader(String sourceFile, RecordSender recordSender) {

        Configuration configuration = s3Client.getConf();
        Path path = s3Client.path(sourceFile);
        JobConf conf = new JobConf(configuration);
        OrcSerde serde = new OrcSerde();
        serde.initialize(conf, p);

        StructObjectInspector inspector;
        try {
            inspector = (StructObjectInspector) serde.getObjectInspector();
            InputFormat<?, ?> in = new OrcInputFormat();
            FileInputFormat.setInputPaths(conf, path.toString());
            InputSplit[] splits = in.getSplits(conf, 1);
            reader = in.getRecordReader(splits[0], conf, Reporter.NULL);
            Object key = reader.createKey();
            Object value = reader.createValue();
            // 获取列信息
            List<? extends StructField> fields = inspector.getAllStructFieldRefs();

            while (reader.next(key, value)) {
                Record record = recordSender.createRecord();
                for (int i = 0; i < columns.size(); i++) {
                    Object field = inspector.getStructFieldData(value, fields.get(i));
                    Column data = addData(field, columns.get(i).getType(), columns.get(i).getIndex(),columns.get(i).getFormat());
                    record.addColumn(data);
                }
                recordSender.sendToWriter(record);
            }

            close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Column addData(Object field, String type, int index, String format) {
        Column columnGenerated;
        try{
            if (index < 0 || field == null){
                columnGenerated = new StringColumn(null);
            }
            String str = field.toString();
            if (StrUtil.isBlank(str)){
                columnGenerated = new StringColumn(null);
            }

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
                case DECIMAL:
                    columnGenerated = new DoubleColumn(new BigDecimal(str));
                    break;
                case BOOLEAN:
                    columnGenerated = new BoolColumn(Boolean.valueOf(str.trim().toLowerCase()));
                    break;
                case DATE:
                    DateWritableV2 dateWritableV2 = (DateWritableV2)field;
                    java.sql.Date date = DateUtil.columnToDate(dateWritableV2.getTimeInSeconds(),null);
                    columnGenerated = new DateColumn(date);
                    break;
                case TIMESTAMP:
                    TimestampWritableV2 timestampWritableV2 = (TimestampWritableV2)field;
                    columnGenerated = new DateColumn(timestampWritableV2.getTimestamp().toSqlTimestamp());
                    break;
                case STRING:
                case VARCHAR:
                case CHAR:
                default:
                    if(customTimeFormat != null){
                        str = DateUtil.timestampToString(DateUtil.columnToDate(str,customTimeFormat));
                    }
                    columnGenerated = new StringColumn(str);
            }
        } catch (Exception e){
            columnGenerated = new StringColumn(null);
        }

        return columnGenerated;
    }

    @Override
    public void close() throws IOException {
        IoUtil.close(reader);
    }
}
