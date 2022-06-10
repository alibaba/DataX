package com.alibaba.datax.plugin.unstructuredstorage.writer;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.csvreader.CsvWriter;


public class TextCsvWriterManager {

    public static UnstructuredWriter produceTextWriter( Writer writer, String fieldDelimiter, Configuration config) {
        return new TextWriterImpl(writer, fieldDelimiter, config);
    }

    public static UnstructuredWriter produceCsvWriter( Writer writer, char fieldDelimiter, Configuration config) {
        return new CsvWriterImpl(writer, fieldDelimiter, config);
    }
}

class CsvWriterImpl implements UnstructuredWriter {
    private static final Logger LOG = LoggerFactory
            .getLogger(CsvWriterImpl.class);
    // csv 严格符合csv语法, 有标准的转义等处理
    private char fieldDelimiter;
    private String lineDelimiter;
    private DataXCsvWriter csvWriter;

    public CsvWriterImpl(Writer writer, char fieldDelimiter, Configuration config) {
        this.fieldDelimiter = fieldDelimiter;
        this.lineDelimiter = config.getString(Key.LINE_DELIMITER, IOUtils.LINE_SEPARATOR);
        this.csvWriter = new DataXCsvWriter(writer, this.fieldDelimiter);
        this.csvWriter.setTextQualifier('"');
        this.csvWriter.setUseTextQualifier(true);
        // warn: in linux is \n , in windows is \r\n
        this.csvWriter.setRecordDelimiter(this.lineDelimiter.charAt(0));

        String csvWriterConfig = config.getString(Key.CSV_WRITER_CONFIG);
        if (StringUtils.isNotBlank(csvWriterConfig)) {
            try {
                HashMap<String, Object> csvWriterConfigMap = JSON.parseObject(csvWriterConfig,
                        new TypeReference<HashMap<String, Object>>() {
                        });
                if (!csvWriterConfigMap.isEmpty()) {
                    // this.csvWriter.setComment(var1);
                    // this.csvWriter.setDelimiter(var1);
                    // this.csvWriter.setEscapeMode(var1);
                    // this.csvWriter.setForceQualifier(var1);
                    // this.csvWriter.setRecordDelimiter(var1);
                    // this.csvWriter.setTextQualifier(var1);
                    // this.csvWriter.setUseTextQualifier(var1);
                    BeanUtils.populate(this.csvWriter, csvWriterConfigMap);
                    LOG.info(String.format("csvwriterConfig is set successfully. After setting, csvwriter:%s", JSON.toJSONString(this.csvWriter)));
                }
            } catch (Exception e) {
                LOG.warn(String.format("invalid csvWriterConfig config: %s, DataX will ignore it.", csvWriterConfig),
                        e);
            }
        }
    }

    @Override
    public void writeOneRecord(List<String> splitedRows) throws IOException {
        if (splitedRows.isEmpty()) {
            LOG.info("Found one record line which is empty.");
        }
        this.csvWriter.writeRecord(splitedRows.toArray(new String[0]));
    }

    @Override
    public void flush() throws IOException {
        this.csvWriter.flush();
    }

    @Override
    public void close() throws IOException {
        this.csvWriter.close();
    }

}

class TextWriterImpl implements UnstructuredWriter {
    private static final Logger LOG = LoggerFactory
            .getLogger(TextWriterImpl.class);
    // text StringUtils的join方式, 简单的字符串拼接
    private String fieldDelimiter;
    private Writer textWriter;
    private String lineDelimiter;

    public TextWriterImpl(Writer writer, String fieldDelimiter, Configuration config) {
        this.fieldDelimiter = fieldDelimiter;
        this.textWriter = writer;
        this.lineDelimiter = config.getString(Key.LINE_DELIMITER, IOUtils.LINE_SEPARATOR);
    }

    @Override
    public void writeOneRecord(List<String> splitedRows) throws IOException {
        if (splitedRows.isEmpty()) {
            LOG.info("Found one record line which is empty.");
        }
        this.textWriter.write(String.format("%s%s",
                StringUtils.join(splitedRows, this.fieldDelimiter),
                this.lineDelimiter));
    }

    @Override
    public void flush() throws IOException {
        this.textWriter.flush();
    }

    @Override
    public void close() throws IOException {
        this.textWriter.close();
    }

}
