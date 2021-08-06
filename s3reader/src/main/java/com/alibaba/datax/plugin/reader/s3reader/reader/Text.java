package com.alibaba.datax.plugin.reader.s3reader.reader;

import cn.hutool.core.util.StrUtil;
import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.plugin.s3common.Constant;
import com.alibaba.datax.plugin.s3common.S3ErrorCode;
import com.alibaba.datax.plugin.s3common.base.ReaderBase;
import com.alibaba.datax.plugin.s3common.entry.ColumnEntry;
import com.alibaba.datax.plugin.s3common.util.*;
import com.alibaba.fastjson.JSON;
import com.csvreader.CsvReader;
import io.airlift.compress.gzip.JdkGzipCodec;
import io.airlift.compress.lz4.Lz4Codec;
import io.airlift.compress.snappy.SnappyCodec;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;

/**
 * Author: duhanmin
 * Description:
 * Date: 2021/7/7 13:46
 */
@Slf4j
public class Text implements ReaderBase<String> {

    public static HashMap<String, Object> csvReaderConfigMap;
    private BufferedReader reader = null;
    private CsvReader csvReader = null;

    private String fieldDelimiter;
    private String compression;
    private List<ColumnEntry> columns;
    private AWSExecutor s3Client ;
    private InputStream inputStream;
    private FileSystem fileSystem;

    public Text(String fieldDelimiter, String compression, List<ColumnEntry> columns, AWSExecutor s3Client) {
        this.fieldDelimiter = fieldDelimiter;
        this.compression = compression;
        this.columns = columns;
        this.s3Client = s3Client;
    }

    @Override
    public void reader(String sourceFile, RecordSender recordSender) {
        try {
            Path path = s3Client.path(sourceFile);
            Configuration conf = s3Client.getConf();
            fileSystem = FileSystem.get(path.toUri(),conf);
            this.inputStream = fileSystem.open(path);
            compressType(conf);
            String[] parseRows;
            while ((parseRows = splitBufferedReader(csvReader)) != null) {
                record(recordSender, columns, parseRows);
            }
            close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close(){
        if (null != csvReader)
            csvReader.close();
        IOUtils.closeQuietly(reader);
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(fileSystem);
    }

    @Override
    public Column addData(String str, String type, int index, String format) {
        if(str == null || str.length() == 0 || type == null || str.equals(HdfsUtil.NULL_VALUE)){
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
            case BOOLEAN:
                columnGenerated = new BoolColumn(Boolean.valueOf(str.trim().toLowerCase()));
                break;
            case DATE:
                columnGenerated = new DateColumn(DateUtil.columnToDate(str,customTimeFormat));
                break;
            case TIMESTAMP:
                columnGenerated = new DateColumn(DateUtil.columnToTimestamp(str,customTimeFormat));
                break;
            case DECIMAL:
                columnGenerated = new DoubleColumn(new BigDecimal(str));
                break;
            default:
                columnGenerated = new StringColumn(str);
        }
        return columnGenerated;
    }

    /**
     *
     * @param csvReader
     * @return
     * @throws IOException
     */
    private String[] splitBufferedReader(CsvReader csvReader) {
        String[] splitedResult = null;
        try {
            if (csvReader.readRecord()) {
                splitedResult = csvReader.getValues();
            }
            return splitedResult;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Record record(RecordSender recordSender,List<ColumnEntry> columnConfigs, String[] sourceLine) {
        Record record = recordSender.createRecord();
        Column columnGenerated;
        // 创建都为String类型column的record
        if (null == columnConfigs || columnConfigs.size() == 0) {
            for (String columnValue : sourceLine) {
                columnGenerated = new StringColumn(columnValue);
                record.addColumn(columnGenerated);
            }
            recordSender.sendToWriter(record);
        } else {
            try {
                for (ColumnEntry columnConfig : columnConfigs) {
                    String columnType = columnConfig.getType();
                    Integer columnIndex = columnConfig.getIndex();

                    String columnValue = null;

                    if (null != columnIndex) {
                        if (columnIndex >= sourceLine.length) {
                            String message = String.format("您尝试读取的列越界,源文件该行有 [%s] 列,您尝试读取第 [%s] 列, 数据详情[%s]", sourceLine.length, columnIndex + 1, StrUtil.join(",",sourceLine));
                            throw new IndexOutOfBoundsException(message);
                        }
                        columnValue = sourceLine[columnIndex];
                    }
                    columnGenerated = addData(columnValue,columnType,0,columnConfig.getFormat());
                    record.addColumn(columnGenerated);
                }
                recordSender.sendToWriter(record);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return record;
    }

    private void compressType(Configuration conf) {
        ECompressType compressType = ECompressType.getByTypeAndFileType(compression, "text");
        try {
            InputStream compressionInputStream;
            if(ECompressType.TEXT_NONE.equals(compressType)){
                compressionInputStream = inputStream;
            }else if (ECompressType.TEXT_GZIP.equals(compressType)){
                compressionInputStream = new JdkGzipCodec().createInputStream(inputStream);
            } else if(ECompressType.TEXT_BZIP2.equals(compressType)){
                BZip2Codec bZip2Codec = new BZip2Codec();
                bZip2Codec.setConf(conf);
                compressionInputStream = bZip2Codec.createInputStream(inputStream);
            }  else if(ECompressType.TEXT_LZ4.equals(compressType)){
                compressionInputStream = new Lz4Codec().createInputStream(inputStream);
            } else if (ECompressType.TEXT_SNAPPY.equals(compressType)) {
                compressionInputStream = new SnappyCodec().createInputStream(inputStream);
            }else {
                throw DataXException.asDataXException(S3ErrorCode.ILLEGAL_VALUE,
                        String.format("仅支持 gzip, bzip2, lzo, snappy 文件压缩格式 , 不支持您配置的文件压缩格式: [%s]", compression));
            }
            reader = new BufferedReader(new InputStreamReader(compressionInputStream, Constant.CHARSET), Constant.DEFAULT_BUFFER_SIZE);
            csvReader();

        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    /**
     * csv实例化
     */
    private void csvReader(){
        this.csvReader = new CsvReader(reader);
        this.csvReader.setDelimiter(fieldDelimiter.charAt(0));

        if(null != csvReaderConfigMap && !csvReaderConfigMap.isEmpty()){
            try {
                BeanUtils.populate(this.csvReader,csvReaderConfigMap);
                log.info(String.format("csvReaderConfig设置成功,设置后CsvReader:{}", JSON.toJSONString(this.csvReader)));
            } catch (Exception e) {
                log.info(String.format("WARN!!!!忽略csvReaderConfig配置!通过BeanUtils.populate配置您的csvReaderConfig发生异常,您配置的值为: {};请检查您的配置!CsvReader使用默认值{}",
                        JSON.toJSONString(csvReaderConfigMap),JSON.toJSONString(this.csvReader)));
            }
        }else {
            //默认关闭安全模式, 放开10W字节的限制
            this.csvReader.setSafetySwitch(false);
            log.info(String.format("CsvReader使用默认值{},csvReaderConfig值为{}",JSON.toJSONString(csvReader),JSON.toJSONString(csvReaderConfigMap)));
        }
    }
}
