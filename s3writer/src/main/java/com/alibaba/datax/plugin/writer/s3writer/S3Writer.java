package com.alibaba.datax.plugin.writer.s3writer;

import cn.hutool.core.text.StrPool;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.s3common.Constant;
import com.alibaba.datax.plugin.s3common.S3ErrorCode;
import com.alibaba.datax.plugin.s3common.base.WriterBase;
import com.alibaba.datax.plugin.s3common.util.AWSExecutor;
import com.alibaba.datax.plugin.s3common.util.ECompressType;
import com.alibaba.datax.plugin.writer.s3writer.writer.Avro;
import com.alibaba.datax.plugin.writer.s3writer.writer.Orc;
import com.alibaba.datax.plugin.writer.s3writer.writer.Parquet;
import com.alibaba.datax.plugin.writer.s3writer.writer.Text;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * S3 Writer
 *
 * Author: duhanmin
 * Description:
 * Date: 2021/5/19 11:53
 */
public class S3Writer extends Writer {
    @Slf4j
    public static class Job extends Writer.Job {

        private Configuration writerSliceConfig = null;
        private AWSExecutor s3Client = null;
        private String bucket;
        private String path;
        private String writeMode;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.validateParameter();
            String accessKey = this.writerSliceConfig.getNecessaryValue(Constant.ACCESSKEY, S3ErrorCode.REQUIRED_VALUE);
            String secretKey = this.writerSliceConfig.getNecessaryValue(Constant.SECRETKEY, S3ErrorCode.REQUIRED_VALUE);
            this.s3Client = new AWSExecutor(accessKey,secretKey);
        }

        private void validateParameter() {
            bucket = this.writerSliceConfig.getNecessaryValue(Constant.BUCKET, S3ErrorCode.REQUIRED_VALUE);
            path = this.writerSliceConfig.getNecessaryValue(Constant.PATH, S3ErrorCode.REQUIRED_VALUE).toLowerCase(Locale.ROOT).trim();
            writeMode = this.writerSliceConfig.getString(Constant.WRITE_MODE, "append").toLowerCase(Locale.ROOT).trim();
            // warn: do not support compress!!
            this.writerSliceConfig.getNecessaryValue(Constant.COMPRESSION,S3ErrorCode.REQUIRED_VALUE);
        }

        @Override
        public void prepare() {
            log.info("begin do prepare...");
            // warn: bucket is not exists, create it
            // warn: do not create bucket for user
            if (!s3Client.exist(bucket)) {
                throw DataXException.asDataXException(S3ErrorCode.ILLEGAL_VALUE, String.format("您配置的bucket [%s] 不存在, 请您确认您的配置项.", bucket));
            }
            log.info("access control details {}.", s3Client.acl(bucket));
            // truncate option handler
            if ("truncate".equals(writeMode)) {
                log.info("由于您配置了writeMode truncate, 开始清理 {} 下面以 {} 开头的Object", bucket, path);
                // warn: 默认情况下，如果Bucket中的Object数量大于100，则只会返回100个Object
                while (true) {
                    log.info("list objects with listObject(bucket, object)");
                    List<String> keyList = s3Client.list(bucket, path);
                    if (keyList.isEmpty()) {
                        break;
                    }
                    s3Client.delete(bucket, keyList);
                }
            } else if ("append".equals(writeMode)) {
                log.info("由于您配置了writeMode append, 写入前不做清理工作, 数据写入Bucket {} 下, 写入相应Object的前缀为 {}", bucket, path);
            } else {
                throw DataXException.asDataXException(S3ErrorCode.ILLEGAL_VALUE,
                        String.format("您配置了不支持的writeMode:%s ", writeMode));
            }
        }

        @Override
        public void destroy() {
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            log.info("begin do split...");
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            Configuration splitedTaskConfig = this.writerSliceConfig.clone();
            for (int i = 0; i < mandatoryNumber; i++) {
                writerSplitConfigs.add(splitedTaskConfig);
            }
            return writerSplitConfigs;
        }
    }

    @Slf4j
    public static class Task extends Writer.Task {

        private WriterBase base;

        @Override
        public void init() {
            Configuration writerSliceConfig = this.getPluginJobConf();
            String accessKey = writerSliceConfig.getString(Constant.ACCESSKEY);
            String secretKey = writerSliceConfig.getString(Constant.SECRETKEY);
            AWSExecutor s3Client = new AWSExecutor(accessKey,secretKey);
            String bucket = writerSliceConfig.getString(Constant.BUCKET);
            s3Client.setBucket(bucket);
            org.apache.hadoop.conf.Configuration conf = s3Client.getConf();

            String path = writerSliceConfig.getString(Constant.PATH);
            if (StrUtil.startWith(path,StrPool.SLASH))
                throw new RuntimeException("path路径错误,开头不能出现/");
            else if (!StrUtil.endWith(path,StrPool.SLASH))
                path = path + StrPool.SLASH;
            String stored = writerSliceConfig.getString(Constant.STORED, ECompressType.TEXT_NONE.getFileType()).toLowerCase(Locale.ROOT).trim();
            String compression = writerSliceConfig.getString(Constant.COMPRESSION, ECompressType.TEXT_NONE.getType()).toLowerCase(Locale.ROOT).trim();

            List<Configuration> columns = writerSliceConfig.getListConfiguration(Constant.COLUMN);
            List<String> fullColumnNames = new ArrayList<>();
            List<String> fullColumnTypes = new ArrayList<>();
            if (null == columns || columns.size() == 0) {
                throw new RuntimeException("您需要指定 columns");
            }else{
                for (Configuration eachColumnConf : columns) {
                    fullColumnNames.add(eachColumnConf.getNecessaryValue(Constant.NAME, S3ErrorCode.REQUIRED_VALUE));
                    fullColumnTypes.add(eachColumnConf.getNecessaryValue(Constant.TYPE, S3ErrorCode.REQUIRED_VALUE));
                }
            }

            Path tmpFile = s3Client.fileTypePath(path,stored);

            if (stored.equals(ECompressType.TEXT_NONE.getFileType())){
                String fieldDelimiter = writerSliceConfig.getString(Constant.FIELD_DELIMITER_KEY, Constant.FIELD_DELIMITER);
               this.base = new Text(fieldDelimiter,fullColumnTypes,tmpFile,compression,conf);
            } else if (stored.equals((ECompressType.PARQUET_NONE.getFileType()))){
                this.base = new Parquet(tmpFile,compression,fullColumnNames,fullColumnTypes,conf);
            } else if (stored.equals((ECompressType.ORC_NONE.getFileType()))){
                this.base = new Orc(tmpFile,compression,fullColumnNames,fullColumnTypes,conf);
            } else if (stored.equals((ECompressType.AVRO_NONE.getFileType()))){
                this.base = new Avro(path,tmpFile,compression,fullColumnNames,fullColumnTypes,conf);
            }else throw new RuntimeException("不受支持的stored");
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            log.info("start do write");
            try {
                if (ObjectUtil.isNotNull(base))
                    base.writer(lineReceiver);
            } catch (IOException e) {
                throw new RuntimeException("写入过程中出错.",e);
            }
            log.info("end do write");
        }

        @Override
        public void destroy() {
            try {
                if (base != null)
                    base.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
