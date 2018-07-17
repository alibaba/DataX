package com.alibaba.datax.plugin.unstructuredstorage.writer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.google.common.collect.Sets;

public class UnstructuredStorageWriterUtil {
    private UnstructuredStorageWriterUtil() {

    }

    private static final Logger LOG = LoggerFactory
            .getLogger(UnstructuredStorageWriterUtil.class);

    /**
     * check parameter: writeMode, encoding, compress, filedDelimiter
     * */
    public static void validateParameter(Configuration writerConfiguration) {
        // writeMode check
        String writeMode = writerConfiguration.getNecessaryValue(
                Key.WRITE_MODE,
                UnstructuredStorageWriterErrorCode.REQUIRED_VALUE);
        writeMode = writeMode.trim();
        Set<String> supportedWriteModes = Sets.newHashSet("truncate", "append",
                "nonConflict");
        if (!supportedWriteModes.contains(writeMode)) {
            throw DataXException
                    .asDataXException(
                            UnstructuredStorageWriterErrorCode.ILLEGAL_VALUE,
                            String.format(
                                    "仅支持 truncate, append, nonConflict 三种模式, 不支持您配置的 writeMode 模式 : [%s]",
                                    writeMode));
        }
        writerConfiguration.set(Key.WRITE_MODE, writeMode);

        // encoding check
        String encoding = writerConfiguration.getString(Key.ENCODING);
        if (StringUtils.isBlank(encoding)) {
            // like "  ", null
            LOG.warn(String.format("您的encoding配置为空, 将使用默认值[%s]",
                    Constant.DEFAULT_ENCODING));
            writerConfiguration.set(Key.ENCODING, Constant.DEFAULT_ENCODING);
        } else {
            try {
                encoding = encoding.trim();
                writerConfiguration.set(Key.ENCODING, encoding);
                Charsets.toCharset(encoding);
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        UnstructuredStorageWriterErrorCode.ILLEGAL_VALUE,
                        String.format("不支持您配置的编码格式:[%s]", encoding), e);
            }
        }

        // only support compress types
        String compress = writerConfiguration.getString(Key.COMPRESS);
        if (StringUtils.isBlank(compress)) {
            writerConfiguration.set(Key.COMPRESS, null);
        } else {
            Set<String> supportedCompress = Sets.newHashSet("gzip", "bzip2");
            if (!supportedCompress.contains(compress.toLowerCase().trim())) {
                String message = String.format(
                        "仅支持 [%s] 文件压缩格式 , 不支持您配置的文件压缩格式: [%s]",
                        StringUtils.join(supportedCompress, ","), compress);
                throw DataXException.asDataXException(
                        UnstructuredStorageWriterErrorCode.ILLEGAL_VALUE,
                        String.format(message, compress));
            }
        }

        // fieldDelimiter check
        String delimiterInStr = writerConfiguration
                .getString(Key.FIELD_DELIMITER);
        // warn: if have, length must be one
        if (null != delimiterInStr && 1 != delimiterInStr.length()) {
            throw DataXException.asDataXException(
                    UnstructuredStorageWriterErrorCode.ILLEGAL_VALUE,
                    String.format("仅仅支持单字符切分, 您配置的切分为 : [%s]", delimiterInStr));
        }
        if (null == delimiterInStr) {
            LOG.warn(String.format("您没有配置列分隔符, 使用默认值[%s]",
                    Constant.DEFAULT_FIELD_DELIMITER));
            writerConfiguration.set(Key.FIELD_DELIMITER,
                    Constant.DEFAULT_FIELD_DELIMITER);
        }

        // fileFormat check
        String fileFormat = writerConfiguration.getString(Key.FILE_FORMAT,
                Constant.FILE_FORMAT_TEXT);
        if (!Constant.FILE_FORMAT_CSV.equals(fileFormat)
                && !Constant.FILE_FORMAT_TEXT.equals(fileFormat)) {
            throw DataXException.asDataXException(
                    UnstructuredStorageWriterErrorCode.ILLEGAL_VALUE, String
                            .format("您配置的fileFormat [%s]错误, 支持csv, text两种.",
                                    fileFormat));
        }
    }

    public static List<Configuration> split(Configuration writerSliceConfig,
            Set<String> originAllFileExists, int mandatoryNumber) {
        LOG.info("begin do split...");
        Set<String> allFileExists = new HashSet<String>();
        allFileExists.addAll(originAllFileExists);
        List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
        String filePrefix = writerSliceConfig.getString(Key.FILE_NAME);

        String fileSuffix;
        for (int i = 0; i < mandatoryNumber; i++) {
            // handle same file name
            Configuration splitedTaskConfig = writerSliceConfig.clone();
            String fullFileName = null;
            fileSuffix = UUID.randomUUID().toString().replace('-', '_');
            fullFileName = String.format("%s__%s", filePrefix, fileSuffix);
            while (allFileExists.contains(fullFileName)) {
                fileSuffix = UUID.randomUUID().toString().replace('-', '_');
                fullFileName = String.format("%s__%s", filePrefix, fileSuffix);
            }
            allFileExists.add(fullFileName);
            splitedTaskConfig.set(Key.FILE_NAME, fullFileName);
            LOG.info(String
                    .format("splited write file name:[%s]", fullFileName));
            writerSplitConfigs.add(splitedTaskConfig);
        }
        LOG.info("end do split.");
        return writerSplitConfigs;
    }

    public static String buildFilePath(String path, String fileName,
            String suffix) {
        boolean isEndWithSeparator = false;
        switch (IOUtils.DIR_SEPARATOR) {
        case IOUtils.DIR_SEPARATOR_UNIX:
            isEndWithSeparator = path.endsWith(String
                    .valueOf(IOUtils.DIR_SEPARATOR));
            break;
        case IOUtils.DIR_SEPARATOR_WINDOWS:
            isEndWithSeparator = path.endsWith(String
                    .valueOf(IOUtils.DIR_SEPARATOR_WINDOWS));
            break;
        default:
            break;
        }
        if (!isEndWithSeparator) {
            path = path + IOUtils.DIR_SEPARATOR;
        }
        if (null == suffix) {
            suffix = "";
        } else {
            suffix = suffix.trim();
        }
        return String.format("%s%s%s", path, fileName, suffix);
    }

    public static void writeToStream(RecordReceiver lineReceiver,
            OutputStream outputStream, Configuration config, String context,
            TaskPluginCollector taskPluginCollector) {
        String encoding = config.getString(Key.ENCODING,
                Constant.DEFAULT_ENCODING);
        // handle blank encoding
        if (StringUtils.isBlank(encoding)) {
            LOG.warn(String.format("您配置的encoding为[%s], 使用默认值[%s]", encoding,
                    Constant.DEFAULT_ENCODING));
            encoding = Constant.DEFAULT_ENCODING;
        }
        String compress = config.getString(Key.COMPRESS);

        BufferedWriter writer = null;
        // compress logic
        try {
            if (null == compress) {
                writer = new BufferedWriter(new OutputStreamWriter(
                        outputStream, encoding));
            } else {
                // TODO more compress
                if ("gzip".equalsIgnoreCase(compress)) {
                    CompressorOutputStream compressorOutputStream = new GzipCompressorOutputStream(
                            outputStream);
                    writer = new BufferedWriter(new OutputStreamWriter(
                            compressorOutputStream, encoding));
                } else if ("bzip2".equalsIgnoreCase(compress)) {
                    CompressorOutputStream compressorOutputStream = new BZip2CompressorOutputStream(
                            outputStream);
                    writer = new BufferedWriter(new OutputStreamWriter(
                            compressorOutputStream, encoding));
                } else {
                    throw DataXException
                            .asDataXException(
                                    UnstructuredStorageWriterErrorCode.ILLEGAL_VALUE,
                                    String.format(
                                            "仅支持 gzip, bzip2 文件压缩格式 , 不支持您配置的文件压缩格式: [%s]",
                                            compress));
                }
            }
            UnstructuredStorageWriterUtil.doWriteToStream(lineReceiver, writer,
                    context, config, taskPluginCollector);
        } catch (UnsupportedEncodingException uee) {
            throw DataXException
                    .asDataXException(
                            UnstructuredStorageWriterErrorCode.Write_FILE_WITH_CHARSET_ERROR,
                            String.format("不支持的编码格式 : [%s]", encoding), uee);
        } catch (NullPointerException e) {
            throw DataXException.asDataXException(
                    UnstructuredStorageWriterErrorCode.RUNTIME_EXCEPTION,
                    "运行时错误, 请联系我们", e);
        } catch (IOException e) {
            throw DataXException.asDataXException(
                    UnstructuredStorageWriterErrorCode.Write_FILE_IO_ERROR,
                    String.format("流写入错误 : [%s]", context), e);
        } finally {
            IOUtils.closeQuietly(writer);
        }
    }

    private static void doWriteToStream(RecordReceiver lineReceiver,
            BufferedWriter writer, String contex, Configuration config,
            TaskPluginCollector taskPluginCollector) throws IOException {

        String nullFormat = config.getString(Key.NULL_FORMAT);

        // 兼容format & dataFormat
        String dateFormat = config.getString(Key.DATE_FORMAT);
        DateFormat dateParse = null; // warn: 可能不兼容
        if (StringUtils.isNotBlank(dateFormat)) {
            dateParse = new SimpleDateFormat(dateFormat);
        }

        // warn: default false
        String fileFormat = config.getString(Key.FILE_FORMAT,
                Constant.FILE_FORMAT_TEXT);

        String delimiterInStr = config.getString(Key.FIELD_DELIMITER);
        if (null != delimiterInStr && 1 != delimiterInStr.length()) {
            throw DataXException.asDataXException(
                    UnstructuredStorageWriterErrorCode.ILLEGAL_VALUE,
                    String.format("仅仅支持单字符切分, 您配置的切分为 : [%s]", delimiterInStr));
        }
        if (null == delimiterInStr) {
            LOG.warn(String.format("您没有配置列分隔符, 使用默认值[%s]",
                    Constant.DEFAULT_FIELD_DELIMITER));
        }

        // warn: fieldDelimiter could not be '' for no fieldDelimiter
        char fieldDelimiter = config.getChar(Key.FIELD_DELIMITER,
                Constant.DEFAULT_FIELD_DELIMITER);

        UnstructuredWriter unstructuredWriter = TextCsvWriterManager
                .produceUnstructuredWriter(fileFormat, fieldDelimiter, writer);

        List<String> headers = config.getList(Key.HEADER, String.class);
        if (null != headers && !headers.isEmpty()) {
            unstructuredWriter.writeOneRecord(headers);
        }

        Record record = null;
        while ((record = lineReceiver.getFromReader()) != null) {
            UnstructuredStorageWriterUtil.transportOneRecord(record,
                    nullFormat, dateParse, taskPluginCollector,
                    unstructuredWriter);
        }

        // warn:由调用方控制流的关闭
        // IOUtils.closeQuietly(unstructuredWriter);
    }

    /**
     * 异常表示脏数据
     * */
    public static void transportOneRecord(Record record, String nullFormat,
            DateFormat dateParse, TaskPluginCollector taskPluginCollector,
            UnstructuredWriter unstructuredWriter) {
        // warn: default is null
        if (null == nullFormat) {
            nullFormat = "null";
        }
        try {
            List<String> splitedRows = new ArrayList<String>();
            int recordLength = record.getColumnNumber();
            if (0 != recordLength) {
                Column column;
                for (int i = 0; i < recordLength; i++) {
                    column = record.getColumn(i);
                    if (null != column.getRawData()) {
                        boolean isDateColumn = column instanceof DateColumn;
                        if (!isDateColumn) {
                            splitedRows.add(column.asString());
                        } else {
                            if (null != dateParse) {
                                splitedRows.add(dateParse.format(column
                                        .asDate()));
                            } else {
                                splitedRows.add(column.asString());
                            }
                        }
                    } else {
                        // warn: it's all ok if nullFormat is null
                        splitedRows.add(nullFormat);
                    }
                }
            }
            unstructuredWriter.writeOneRecord(splitedRows);
        } catch (Exception e) {
            // warn: dirty data
            taskPluginCollector.collectDirtyRecord(record, e);
        }
    }
}
