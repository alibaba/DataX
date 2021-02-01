
package com.alibaba.datax.plugin.writer.streamwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class StreamWriter extends Writer {
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            String path = this.originalConfig.getString(Key.PATH, null);
            String fileName = this.originalConfig.getString(Key.FILE_NAME, null);

            if(StringUtils.isNoneBlank(path) && StringUtils.isNoneBlank(fileName)) {
                validateParameter(path, fileName);
            }
        }

        private void validateParameter(String path, String fileName) {
            try {
                // warn: 这里用户需要配一个目录
                File dir = new File(path);
                if (dir.isFile()) {
                    throw DataXException
                            .asDataXException(
                                    StreamWriterErrorCode.ILLEGAL_VALUE,
                                    String.format(
                                            "您配置的path: [%s] 不是一个合法的目录, 请您注意文件重名, 不合法目录名等情况.",
                                            path));
                }
                if (!dir.exists()) {
                    boolean createdOk = dir.mkdirs();
                    if (!createdOk) {
                        throw DataXException
                                .asDataXException(
                                        StreamWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                                        String.format("您指定的文件路径 : [%s] 创建失败.",
                                                path));
                    }
                }

                String fileFullPath = buildFilePath(path, fileName);
                File newFile = new File(fileFullPath);
                if(newFile.exists()) {
                    try {
                        FileUtils.forceDelete(newFile);
                    } catch (IOException e) {
                        throw DataXException.asDataXException(
                                StreamWriterErrorCode.RUNTIME_EXCEPTION,
                                String.format("删除文件失败 : [%s] ", fileFullPath), e);
                    }
                }
            } catch (SecurityException se) {
                throw DataXException.asDataXException(
                        StreamWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限创建文件路径 : [%s] ", path), se);
            }
        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                writerSplitConfigs.add(this.originalConfig);
            }

            return writerSplitConfigs;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);

        private static final String NEWLINE_FLAG = System.getProperty("line.separator", "\n");

        private Configuration writerSliceConfig;

        private String fieldDelimiter;
        private boolean print;

        private String path;
        private String fileName;

        private long recordNumBeforSleep;
        private long sleepTime;



        @Override
        public void init() {
            this.writerSliceConfig = getPluginJobConf();

            this.fieldDelimiter = this.writerSliceConfig.getString(
                    Key.FIELD_DELIMITER, "\t");
            this.print = this.writerSliceConfig.getBool(Key.PRINT, true);

            this.path = this.writerSliceConfig.getString(Key.PATH, null);
            this.fileName = this.writerSliceConfig.getString(Key.FILE_NAME, null);
            this.recordNumBeforSleep = this.writerSliceConfig.getLong(Key.RECORD_NUM_BEFORE_SLEEP, 0);
            this.sleepTime = this.writerSliceConfig.getLong(Key.SLEEP_TIME, 0);
            if(recordNumBeforSleep < 0) {
                throw DataXException.asDataXException(StreamWriterErrorCode.CONFIG_INVALID_EXCEPTION, "recordNumber 不能为负值");
            }
            if(sleepTime <0) {
                throw DataXException.asDataXException(StreamWriterErrorCode.CONFIG_INVALID_EXCEPTION, "sleep 不能为负值");
            }

        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {


                if(StringUtils.isNoneBlank(path) && StringUtils.isNoneBlank(fileName)) {
                    writeToFile(recordReceiver,path, fileName, recordNumBeforSleep, sleepTime);
                } else {
                    try {
                        BufferedWriter writer = new BufferedWriter(
                                new OutputStreamWriter(System.out, "UTF-8"));

                        Record record;
                        while ((record = recordReceiver.getFromReader()) != null) {
                            if (this.print) {
                                writer.write(recordToString(record));
                            } else {
                        /* do nothing */
                            }
                        }
                        writer.flush();

                    } catch (Exception e) {
                        throw DataXException.asDataXException(StreamWriterErrorCode.RUNTIME_EXCEPTION, e);
                    }
                }
        }

        private void writeToFile(RecordReceiver recordReceiver, String path, String fileName,
                                 long recordNumBeforSleep, long sleepTime) {

            LOG.info("begin do write...");
            String fileFullPath = buildFilePath(path, fileName);
            LOG.info(String.format("write to file : [%s]", fileFullPath));
            BufferedWriter writer = null;
            try {
                File newFile = new File(fileFullPath);
                newFile.createNewFile();

                writer = new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(newFile, true), "UTF-8"));

                Record record;
                int count =0;
                while ((record = recordReceiver.getFromReader()) != null) {
                    if(recordNumBeforSleep > 0 && sleepTime >0 &&count == recordNumBeforSleep) {
                        LOG.info("StreamWriter start to sleep ... recordNumBeforSleep={},sleepTime={}",recordNumBeforSleep,sleepTime);
                        try {
                            Thread.sleep(sleepTime * 1000l);
                        } catch (InterruptedException e) {
                        }
                    }
                   writer.write(recordToString(record));
                   count++;
                }
                writer.flush();
            } catch (Exception e) {
                throw DataXException.asDataXException(StreamWriterErrorCode.RUNTIME_EXCEPTION, e);
            } finally {
                IOUtils.closeQuietly(writer);
            }
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

        private String recordToString(Record record) {
            int recordLength = record.getColumnNumber();
            if (0 == recordLength) {
                return NEWLINE_FLAG;
            }

            Column column;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < recordLength; i++) {
                column = record.getColumn(i);
                sb.append(column.asString()).append(fieldDelimiter);
            }
            sb.setLength(sb.length() - 1);
            sb.append(NEWLINE_FLAG);

            return sb.toString();
        }
    }

    private static String buildFilePath(String path, String fileName) {
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
        return String.format("%s%s", path, fileName);
    }
}
