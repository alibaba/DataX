package com.alibaba.datax.plugin.reader.s3reader;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.s3reader.reader.*;
import com.alibaba.datax.plugin.s3common.Constant;
import com.alibaba.datax.plugin.s3common.S3ErrorCode;
import com.alibaba.datax.plugin.s3common.base.ReaderBase;
import com.alibaba.datax.plugin.s3common.util.AWSExecutor;
import com.alibaba.datax.plugin.s3common.util.ColumnTypeUtil;
import com.alibaba.datax.plugin.s3common.util.ECompressType;
import com.alibaba.datax.plugin.s3common.entry.ColumnEntry;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * s3 reader
 *
 * Author: duhanmin
 * Description:
 * Date: 2021/7/7 13:46
 */
public class S3Reader extends Reader {
    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration readerOriginConfig = null;
        private AWSExecutor s3Client = null;
        private String bucket;
        private List<String> paths;

        @Override
        public void init() {
            LOG.debug("init() begin...");
            this.readerOriginConfig = this.getPluginJobConf();
            this.validate();
            LOG.debug("init() ok and end...");
        }

        private void validate() {
            String accessKey = this.readerOriginConfig.getNecessaryValue(Constant.ACCESSKEY, S3ErrorCode.REQUIRED_VALUE);
            String secretKey = this.readerOriginConfig.getNecessaryValue(Constant.SECRETKEY, S3ErrorCode.REQUIRED_VALUE);
            this.s3Client = new AWSExecutor(accessKey,secretKey);

            bucket = this.readerOriginConfig.getNecessaryValue(Constant.BUCKET, S3ErrorCode.REQUIRED_VALUE);
            paths = JSONObject.parseArray(this.readerOriginConfig.getNecessaryValue(Constant.PATH, S3ErrorCode.REQUIRED_VALUE),String.class);

            String stored = this.readerOriginConfig.getString(Constant.STORED, ECompressType.TEXT_NONE.getFileType()).toLowerCase(Locale.ROOT).trim();
            String compression = this.readerOriginConfig.getString(Constant.COMPRESSION, ECompressType.TEXT_NONE.getType()).toLowerCase(Locale.ROOT).trim();

            ECompressType.getByTypeAndFileType(compression,stored);

            List<Configuration> columns = this.readerOriginConfig.getListConfiguration(Constant.COLUMN);
            if (null == columns || columns.size() == 0) {
                throw new RuntimeException("您需要指定 columns");
            }else{
                for (Configuration eachColumnConf : columns) {
                    eachColumnConf.getNecessaryValue(Constant.TYPE, S3ErrorCode.REQUIRED_VALUE);
                    eachColumnConf.getInt(Constant.INDEX);
                }
            }
        }

        @Override
        public void prepare() {
            LOG.debug("prepare()");
        }

        @Override
        public void post() {
            LOG.debug("post()");
        }

        @Override
        public void destroy() {
            LOG.debug("destroy()");
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            LOG.debug("split() begin...");
            List<Configuration> readerSplitConfigs = new ArrayList<>();

            // 将每个单独的 object 作为一个 slice
            for (String object : paths) {
                Configuration splitedConfig = this.readerOriginConfig.clone();
                List<String> list = s3Client.list(bucket, object);
                LOG.info("file:{}",list);
                splitedConfig.set(Constant.SOURCE_FILES, list);
                readerSplitConfigs.add(splitedConfig);
                LOG.info(String.format("object to be read:%s", object));
            }
            LOG.debug("split() ok and end...");
            return readerSplitConfigs;
        }
    }

    public static class Task extends Reader.Task {
        private static Logger LOG = LoggerFactory.getLogger(Reader.Task.class);

        private List<String> sourceFiles;
        private ReaderBase base;

        @Override
        public void init() {
            Configuration readerSliceConfig = this.getPluginJobConf();
            this.sourceFiles = readerSliceConfig.getList(Constant.SOURCE_FILES, String.class);
            String accessKey = readerSliceConfig.getString(Constant.ACCESSKEY);
            String secretKey = readerSliceConfig.getString(Constant.SECRETKEY);
            AWSExecutor s3Client = new AWSExecutor(accessKey,secretKey);
            String bucket = readerSliceConfig.getString(Constant.BUCKET);
            s3Client.setBucket(bucket);

            String stored = readerSliceConfig.getString(Constant.STORED, ECompressType.TEXT_NONE.getFileType()).toLowerCase(Locale.ROOT).trim();
            String compression = readerSliceConfig.getString(Constant.COMPRESSION, ECompressType.TEXT_NONE.getType()).toLowerCase(Locale.ROOT).trim();
            List<ColumnEntry> columns = ColumnTypeUtil.getListColumnEntry(readerSliceConfig, Constant.COLUMN);

            if (stored.equals(ECompressType.TEXT_NONE.getFileType())){
                String fieldDelimiter = readerSliceConfig.getString(Constant.FIELD_DELIMITER_KEY, Constant.FIELD_DELIMITER);
                this.base = new Text(fieldDelimiter,compression,columns,s3Client);
            } else if (stored.equals((ECompressType.PARQUET_NONE.getFileType()))){
                this.base = new Parquet(columns,s3Client);
            } else if (stored.equals((ECompressType.ORC_NONE.getFileType()))){
                this.base = new Orc(columns,s3Client);
            } else if (stored.equals((ECompressType.AVRO_NONE.getFileType()))){
                this.base = new Avro(columns,s3Client);
            }
        }

        @Override
        public void startRead(RecordSender recordSender) {
            LOG.debug("read start");
            for (String sourceFile:sourceFiles) {
                base.reader(sourceFile,recordSender);
            }
            LOG.debug("read end");
        }

        @Override
        public void destroy() {

        }
    }
}
