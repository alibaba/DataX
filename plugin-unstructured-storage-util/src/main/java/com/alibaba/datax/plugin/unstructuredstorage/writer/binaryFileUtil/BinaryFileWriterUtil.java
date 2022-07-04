package com.alibaba.datax.plugin.unstructuredstorage.writer.binaryFileUtil;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderErrorCode;
import com.alibaba.datax.plugin.unstructuredstorage.writer.Key;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredStorageWriterErrorCode;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.*;

/**
 * @Author: guxuan
 * @Date 2022-05-17 17:01
 */
public class BinaryFileWriterUtil {

    private static final Logger LOG = LoggerFactory.getLogger(BinaryFileWriterUtil.class);


    /**
     * 从RecordReceiver获取源文件Bytes数组, 写到目的端
     *
     * @param outputStream:   写文件流
     * @param recordReceiver: RecordReceiver
     */
    public static void writeFileFromRecordReceiver(OutputStream outputStream, RecordReceiver recordReceiver) {
        try {
            Record record;
            while ((record = recordReceiver.getFromReader()) != null) {
                Column column = record.getColumn(0);
                outputStream.write(column.asBytes());
            }
            outputStream.flush();
            LOG.info("End write!!!");
        } catch (IOException e) {
            throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.READ_FILE_IO_ERROR, e);
        }
    }

    /**
     * 校验同步二进制文件的参数
     *
     * @param writerConfiguration: writer的配置
     */
    public static void validateParameter(Configuration writerConfiguration) {
        // writeMode check
        String writeMode = writerConfiguration.getNecessaryValue(
                Key.WRITE_MODE,
                UnstructuredStorageWriterErrorCode.REQUIRED_VALUE);
        writeMode = writeMode.trim();
        Set<String> supportedWriteModes = Sets.newHashSet(TRUNCATE, NOCONFLICT);
        if (!supportedWriteModes.contains(writeMode)) {
            throw DataXException
                    .asDataXException(
                            BinaryFileWriterErrorCode.ILLEGAL_VALUE,
                            String.format("Synchronous binary format file, only supports truncate and nonConflict modes, does not support the writeMode mode you configured: %s", writeMode));
        }
        writerConfiguration.set(Key.WRITE_MODE, writeMode);
    }

    /**
     * 校验文件名是否有重复的,如果有重复的文件名则抛出异常
     * @param fileNameList
     */
    public static void checkFileNameIfRepeatedThrowException(List<String> fileNameList) {
        Set<String> sourceFileNameSet = new HashSet<String>();
        for (String fileName : fileNameList) {
            if (!sourceFileNameSet.contains(fileName)) {
                sourceFileNameSet.add(fileName);
            } else {
                throw DataXException.asDataXException(BinaryFileWriterErrorCode.REPEATED_FILE_NAME,
                        String.format("Source File Name [%s] is repeated!", fileName));
            }
        }
    }

    /**
     *
     * @param readerSplitConfigs
     * @param writerSliceConfig
     * @return 切分后的结果
     */
    public static List<Configuration> split(List<Configuration> readerSplitConfigs, Configuration writerSliceConfig) {
        List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();

        for (Configuration readerSliceConfig : readerSplitConfigs) {
            Configuration splitedTaskConfig = writerSliceConfig.clone();
            String fileName = getFileName(readerSliceConfig.getString(SOURCE_FILE));
            splitedTaskConfig
                    .set(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME, fileName);
            splitedTaskConfig.
                    set(com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.BINARY, true);
            writerSplitConfigs.add(splitedTaskConfig);
        }
        LOG.info("end do split.");
        return writerSplitConfigs;
    }

    /**
     * 根据文件路径获取到文件名, filePath必定包含了文件名
     *
     * @param filePath: 文件路径
     */
    public static String getFileName(String filePath) {
        if (StringUtils.isBlank(filePath)) {
            return null;
        }
        File file = new File(filePath);
        return file.getName();
    }
}
