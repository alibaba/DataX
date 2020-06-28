package com.alibaba.datax.plugin.reader.s3reader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import com.google.common.collect.Sets;
import com.alibaba.datax.plugin.reader.s3reader.util.S3ClientUtil;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * s3 reader
 *
 * @author mengxin.liumx
 * @author L.cm
 */
public class S3Reader extends Reader {
    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration readerOriginConfig = null;

        @Override
        public void init() {
            LOG.debug("init() begin...");
            this.readerOriginConfig = this.getPluginJobConf();
            this.validate();
            LOG.debug("init() ok and end...");
        }

        private void validate() {
            String accessKey = this.readerOriginConfig.getString(Key.ACCESSKEY);
            if (StringUtils.isBlank(accessKey)) {
                throw DataXException.asDataXException(
                        S3ReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        "您需要指定 accessKey");
            }

            String secretKey = this.readerOriginConfig.getString(Key.SECRETKEY);
            if (StringUtils.isBlank(secretKey)) {
                throw DataXException.asDataXException(
                        S3ReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        "您需要指定 secretKey");
            }

            String bucket = this.readerOriginConfig.getString(Key.BUCKET);
            if (StringUtils.isBlank(bucket)) {
                throw DataXException.asDataXException(
                        S3ReaderErrorCode.CONFIG_INVALID_EXCEPTION, "您需要指定 bucket");
            }

            String region = this.readerOriginConfig.getString(Key.REGION);
            if (StringUtils.isBlank(region)) {
                throw DataXException.asDataXException(S3ReaderErrorCode.CONFIG_INVALID_EXCEPTION, "您需要指定 region");
            }

            String object = this.readerOriginConfig.getString(Key.OBJECT);
            if (StringUtils.isBlank(object)) {
                throw DataXException.asDataXException(
                        S3ReaderErrorCode.CONFIG_INVALID_EXCEPTION, "您需要指定 object");
            }

            String fieldDelimiter = this.readerOriginConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.FIELD_DELIMITER);
            // warn: need length 1
            if (null == fieldDelimiter || fieldDelimiter.length() == 0) {
                throw DataXException.asDataXException(
                        S3ReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        "您需要指定 fieldDelimiter");
            }

            String encoding = this.readerOriginConfig
                    .getString(
                            com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING,
                            com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_ENCODING);
            try {
                Charsets.toCharset(encoding);
            } catch (UnsupportedCharsetException uce) {
                throw DataXException.asDataXException(
                        S3ReaderErrorCode.ILLEGAL_VALUE,
                        String.format("不支持的编码格式 : [%s]", encoding), uce);
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        S3ReaderErrorCode.ILLEGAL_VALUE,
                        String.format("运行配置异常 : %s", e.getMessage()), e);
            }

            // 检测是column 是否为 ["*"] 若是则填为空
            List<Configuration> column = this.readerOriginConfig
                    .getListConfiguration(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);
            if (null != column
                    && 1 == column.size()
                    && ("\"*\"".equals(column.get(0).toString()) || "'*'"
                    .equals(column.get(0).toString()))) {
                readerOriginConfig.set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN,
                        new ArrayList<String>());
            } else {
                // column: 1. index type 2.value type 3.when type is Data, may
                // have
                // format
                List<Configuration> columns = this.readerOriginConfig.getListConfiguration(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);

                if (null == columns || columns.size() == 0) {
                    throw DataXException.asDataXException(
                            S3ReaderErrorCode.CONFIG_INVALID_EXCEPTION, "您需要指定 columns");
                }
                for (Configuration eachColumnConf : columns) {
                    eachColumnConf.getNecessaryValue(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.TYPE,
                            S3ReaderErrorCode.REQUIRED_VALUE);
                    Integer columnIndex = eachColumnConf.getInt(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.INDEX);
                    String columnValue = eachColumnConf.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.VALUE);

                    if (null == columnIndex && null == columnValue) {
                        throw DataXException.asDataXException(
                                S3ReaderErrorCode.NO_INDEX_VALUE,
                                "由于您配置了type, 则至少需要配置 index 或 value");
                    }

                    if (null != columnIndex && null != columnValue) {
                        throw DataXException.asDataXException(
                                S3ReaderErrorCode.MIXED_INDEX_VALUE,
                                "您混合配置了index, value, 每一列同时仅能选择其中一种");
                    }

                }
            }

            // only support compress: gzip,bzip2,zip
            String compress = this.readerOriginConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COMPRESS);
            if (StringUtils.isBlank(compress)) {
                this.readerOriginConfig.set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COMPRESS, null);
            } else {
                Set<String> supportedCompress = Sets.newHashSet("gzip", "bzip2", "zip");
                compress = compress.toLowerCase().trim();
                if (!supportedCompress.contains(compress)) {
                    throw DataXException.asDataXException(
                            S3ReaderErrorCode.ILLEGAL_VALUE,
                            String.format("仅支持 gzip, bzip2, zip 文件压缩格式 , 不支持您配置的文件压缩格式: [%s]", compress));
                }
                this.readerOriginConfig.set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COMPRESS, compress);
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
            List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();

            // 将每个单独的 object 作为一个 slice
            List<String> objects = parseOriginObjects(readerOriginConfig.getList(Constant.OBJECT, String.class));
            if (0 == objects.size()) {
                throw DataXException.asDataXException(
                        S3ReaderErrorCode.EMPTY_BUCKET_EXCEPTION,
                        String.format(
                                "未能找到待读取的Object,请确认您的配置项bucket: %s object: %s",
                                this.readerOriginConfig.get(Key.BUCKET),
                                this.readerOriginConfig.get(Key.OBJECT)));
            }

            for (String object : objects) {
                Configuration splitedConfig = this.readerOriginConfig.clone();
                splitedConfig.set(Constant.OBJECT, object);
                readerSplitConfigs.add(splitedConfig);
                LOG.info(String.format("OSS object to be read:%s", object));
            }
            LOG.debug("split() ok and end...");
            return readerSplitConfigs;
        }

        public static void main(String[] args) {
            System.out.println("123123*".substring(0, "123123*".indexOf("*")));
        }

        private List<String> parseOriginObjects(List<String> originObjects) {
            List<String> parsedObjects = new ArrayList<>();
            for (String object : originObjects) {
                // 如果有 * 号， 采用 listObjects，去除 * 作为前缀
                int index = object.indexOf("*");
                if (index > 0) {
                    String prefix = object.substring(0, index);
                    parsedObjects.addAll(getRemoteObjects(prefix));
                } else {
                    parsedObjects.add(object);
                }
            }
            return parsedObjects;
        }

        private List<String> getRemoteObjects(String prefix) {
            LOG.debug(String.format("S3批量文件前缀 : %s", prefix));
            LinkedList<String> remoteObjects = new LinkedList<>();
            S3Client s3Client = S3ClientUtil.initOssClient(readerOriginConfig);
            String bucket = readerOriginConfig.getString(Key.BUCKET);
            try {
                List<String> objectList;
                do {
                    if (remoteObjects.isEmpty()) {
                        objectList = S3ClientUtil.listObjects(s3Client, bucket, prefix);
                    } else {
                        String objectsLast = remoteObjects.getLast();
                        objectList = S3ClientUtil.listObjects(s3Client, bucket, prefix, objectsLast);
                    }
                    for (String objectKey : objectList) {
                        LOG.debug(String.format("找到文件 : %s", objectKey));
                        remoteObjects.add(objectKey);
                    }
                } while (!objectList.isEmpty());
            } catch (IllegalArgumentException e) {
                throw DataXException.asDataXException(S3ReaderErrorCode.S3_EXCEPTION, e.getMessage());
            }
            return remoteObjects;
        }
    }

    public static class Task extends Reader.Task {
        private static Logger LOG = LoggerFactory.getLogger(Reader.Task.class);

        private Configuration readerSliceConfig;

        @Override
        public void startRead(RecordSender recordSender) {
            LOG.debug("read start");
            String object = readerSliceConfig.getString(Key.OBJECT);
            S3Client s3Client = S3ClientUtil.initOssClient(readerSliceConfig);
            String bucket = readerSliceConfig.getString(Key.BUCKET);

            ResponseInputStream<GetObjectResponse> inputStream = S3ClientUtil.getObject(s3Client, bucket, object);
            UnstructuredStorageReaderUtil.readFromStream(inputStream, object,
                    this.readerSliceConfig, recordSender,
                    this.getTaskPluginCollector());
            recordSender.flush();
        }

        @Override
        public void init() {
            this.readerSliceConfig = this.getPluginJobConf();
        }

        @Override
        public void destroy() {

        }
    }
}
