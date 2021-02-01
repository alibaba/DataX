package com.alibaba.datax.plugin.reader.ossreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.ossreader.util.OssUtil;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.google.common.collect.Sets;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Created by mengxin.liumx on 2014/12/7.
 */
public class OssReader extends Reader {
    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(OssReader.Job.class);

        private Configuration readerOriginConfig = null;

        @Override
        public void init() {
            LOG.debug("init() begin...");
            this.readerOriginConfig = this.getPluginJobConf();
            this.validate();
            LOG.debug("init() ok and end...");
        }

        private void validate() {
            String endpoint = this.readerOriginConfig.getString(Key.ENDPOINT);
            if (StringUtils.isBlank(endpoint)) {
                throw DataXException.asDataXException(
                        OssReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        "您需要指定 endpoint");
            }

            String accessId = this.readerOriginConfig.getString(Key.ACCESSID);
            if (StringUtils.isBlank(accessId)) {
                throw DataXException.asDataXException(
                        OssReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        "您需要指定 accessId");
            }

            String accessKey = this.readerOriginConfig.getString(Key.ACCESSKEY);
            if (StringUtils.isBlank(accessKey)) {
                throw DataXException.asDataXException(
                        OssReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        "您需要指定 accessKey");
            }

            String bucket = this.readerOriginConfig.getString(Key.BUCKET);
            if (StringUtils.isBlank(bucket)) {
                throw DataXException.asDataXException(
                        OssReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        "您需要指定 endpoint");
            }

            String object = this.readerOriginConfig.getString(Key.OBJECT);
            if (StringUtils.isBlank(object)) {
                throw DataXException.asDataXException(
                        OssReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        "您需要指定 object");
            }

            String fieldDelimiter = this.readerOriginConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.FIELD_DELIMITER);
            // warn: need length 1
            if (null == fieldDelimiter || fieldDelimiter.length() == 0) {
                throw DataXException.asDataXException(
                        OssReaderErrorCode.CONFIG_INVALID_EXCEPTION,
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
                        OssReaderErrorCode.ILLEGAL_VALUE,
                        String.format("不支持的编码格式 : [%s]", encoding), uce);
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        OssReaderErrorCode.ILLEGAL_VALUE,
                        String.format("运行配置异常 : %s", e.getMessage()), e);
            }

            // 检测是column 是否为 ["*"] 若是则填为空
            List<Configuration> column = this.readerOriginConfig
                    .getListConfiguration(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);
            if (null != column
                    && 1 == column.size()
                    && ("\"*\"".equals(column.get(0).toString()) || "'*'"
                            .equals(column.get(0).toString()))) {
                readerOriginConfig
                        .set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN,
                                new ArrayList<String>());
            } else {
                // column: 1. index type 2.value type 3.when type is Data, may
                // have
                // format
                List<Configuration> columns = this.readerOriginConfig
                        .getListConfiguration(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);

                if (null == columns || columns.size() == 0) {
                    throw DataXException.asDataXException(
                            OssReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                            "您需要指定 columns");
                }

                if (null != columns && columns.size() != 0) {
                    for (Configuration eachColumnConf : columns) {
                        eachColumnConf
                                .getNecessaryValue(
                                        com.alibaba.datax.plugin.unstructuredstorage.reader.Key.TYPE,
                                        OssReaderErrorCode.REQUIRED_VALUE);
                        Integer columnIndex = eachColumnConf
                                .getInt(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.INDEX);
                        String columnValue = eachColumnConf
                                .getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.VALUE);

                        if (null == columnIndex && null == columnValue) {
                            throw DataXException.asDataXException(
                                    OssReaderErrorCode.NO_INDEX_VALUE,
                                    "由于您配置了type, 则至少需要配置 index 或 value");
                        }

                        if (null != columnIndex && null != columnValue) {
                            throw DataXException.asDataXException(
                                    OssReaderErrorCode.MIXED_INDEX_VALUE,
                                    "您混合配置了index, value, 每一列同时仅能选择其中一种");
                        }

                    }
                }
            }

            // only support compress: gzip,bzip2,zip
            String compress = this.readerOriginConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COMPRESS);
            if (StringUtils.isBlank(compress)) {
                this.readerOriginConfig
                        .set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COMPRESS,
                                null);
            } else {
                Set<String> supportedCompress = Sets
                        .newHashSet("gzip", "bzip2", "zip");
                compress = compress.toLowerCase().trim();
                if (!supportedCompress.contains(compress)) {
                    throw DataXException
                            .asDataXException(
                                    OssReaderErrorCode.ILLEGAL_VALUE,
                                    String.format(
                                            "仅支持 gzip, bzip2, zip 文件压缩格式 , 不支持您配置的文件压缩格式: [%s]",
                                            compress));
                }
                this.readerOriginConfig
                        .set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COMPRESS,
                                compress);
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
            List<String> objects = parseOriginObjects(readerOriginConfig
                    .getList(Constant.OBJECT, String.class));
            if (0 == objects.size()) {
                throw DataXException.asDataXException(
                        OssReaderErrorCode.EMPTY_BUCKET_EXCEPTION,
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

        private List<String> parseOriginObjects(List<String> originObjects) {
            List<String> parsedObjects = new ArrayList<String>();

            for (String object : originObjects) {
                int firstMetaChar = (object.indexOf('*') > object.indexOf('?')) ? object
                        .indexOf('*') : object.indexOf('?');

                if (firstMetaChar != -1) {
                    int lastDirSeparator = object.lastIndexOf(
                            IOUtils.DIR_SEPARATOR, firstMetaChar);
                    String parentDir = object
                            .substring(0, lastDirSeparator + 1);
                    List<String> remoteObjects = getRemoteObjects(parentDir);
                    Pattern pattern = Pattern.compile(object.replace("*", ".*")
                            .replace("?", ".?"));

                    for (String remoteObject : remoteObjects) {
                        if (pattern.matcher(remoteObject).matches()) {
                            parsedObjects.add(remoteObject);
                        }
                    }
                } else {
                    parsedObjects.add(object);
                }
            }
            return parsedObjects;
        }

        private List<String> getRemoteObjects(String parentDir)
                throws OSSException, ClientException {

            LOG.debug(String.format("父文件夹 : %s", parentDir));
            List<String> remoteObjects = new ArrayList<String>();
            OSSClient client = OssUtil.initOssClient(readerOriginConfig);
            try {
                ListObjectsRequest listObjectsRequest = new ListObjectsRequest(
                        readerOriginConfig.getString(Key.BUCKET));
                listObjectsRequest.setPrefix(parentDir);
                ObjectListing objectList;
                do {
                    objectList = client.listObjects(listObjectsRequest);
                    for (OSSObjectSummary objectSummary : objectList
                            .getObjectSummaries()) {
                        LOG.debug(String.format("找到文件 : %s",
                                objectSummary.getKey()));
                        remoteObjects.add(objectSummary.getKey());
                    }
                    listObjectsRequest.setMarker(objectList.getNextMarker());
                    LOG.debug(listObjectsRequest.getMarker());
                    LOG.debug(String.valueOf(objectList.isTruncated()));

                } while (objectList.isTruncated());
            } catch (IllegalArgumentException e) {
                throw DataXException.asDataXException(
                        OssReaderErrorCode.OSS_EXCEPTION, e.getMessage());
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
            OSSClient client = OssUtil.initOssClient(readerSliceConfig);

            OSSObject ossObject = client.getObject(
                    readerSliceConfig.getString(Key.BUCKET), object);
            InputStream objectStream = ossObject.getObjectContent();
            UnstructuredStorageReaderUtil.readFromStream(objectStream, object,
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
