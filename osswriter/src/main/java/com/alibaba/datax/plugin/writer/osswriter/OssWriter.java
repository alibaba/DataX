package com.alibaba.datax.plugin.writer.osswriter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.unstructuredstorage.writer.TextCsvWriterManager;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredStorageWriterUtil;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredWriter;
import com.alibaba.datax.plugin.writer.osswriter.util.OssUtil;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;

/**
 * Created by haiwei.luo on 15-02-09.
 */
public class OssWriter extends Writer {
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration writerSliceConfig = null;
        private OSSClient ossClient = null;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.validateParameter();
            this.ossClient = OssUtil.initOssClient(this.writerSliceConfig);
        }

        private void validateParameter() {
            this.writerSliceConfig.getNecessaryValue(Key.ENDPOINT,
                    OssWriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(Key.ACCESSID,
                    OssWriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(Key.ACCESSKEY,
                    OssWriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(Key.BUCKET,
                    OssWriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(Key.OBJECT,
                    OssWriterErrorCode.REQUIRED_VALUE);
            // warn: do not support compress!!
            String compress = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.COMPRESS);
            if (StringUtils.isNotBlank(compress)) {
                String errorMessage = String.format(
                        "OSS写暂时不支持压缩, 该压缩配置项[%s]不起效用", compress);
                LOG.error(errorMessage);
                throw DataXException.asDataXException(
                        OssWriterErrorCode.ILLEGAL_VALUE, errorMessage);

            }
            UnstructuredStorageWriterUtil
                    .validateParameter(this.writerSliceConfig);

        }

        @Override
        public void prepare() {
            LOG.info("begin do prepare...");
            String bucket = this.writerSliceConfig.getString(Key.BUCKET);
            String object = this.writerSliceConfig.getString(Key.OBJECT);
            String writeMode = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.WRITE_MODE);
            // warn: bucket is not exists, create it
            try {
                // warn: do not create bucket for user
                if (!this.ossClient.doesBucketExist(bucket)) {
                    // this.ossClient.createBucket(bucket);
                    String errorMessage = String.format(
                            "您配置的bucket [%s] 不存在, 请您确认您的配置项.", bucket);
                    LOG.error(errorMessage);
                    throw DataXException.asDataXException(
                            OssWriterErrorCode.ILLEGAL_VALUE, errorMessage);
                }
                LOG.info(String.format("access control details [%s].",
                        this.ossClient.getBucketAcl(bucket).toString()));

                // truncate option handler
                if ("truncate".equals(writeMode)) {
                    LOG.info(String
                            .format("由于您配置了writeMode truncate, 开始清理 [%s] 下面以 [%s] 开头的Object",
                                    bucket, object));
                    // warn: 默认情况下，如果Bucket中的Object数量大于100，则只会返回100个Object
                    while (true) {
                        ObjectListing listing = null;
                        LOG.info("list objects with listObject(bucket, object)");
                        listing = this.ossClient.listObjects(bucket, object);
                        List<OSSObjectSummary> objectSummarys = listing
                                .getObjectSummaries();
                        for (OSSObjectSummary objectSummary : objectSummarys) {
                            LOG.info(String.format("delete oss object [%s].",
                                    objectSummary.getKey()));
                            this.ossClient.deleteObject(bucket,
                                    objectSummary.getKey());
                        }
                        if (objectSummarys.isEmpty()) {
                            break;
                        }
                    }
                } else if ("append".equals(writeMode)) {
                    LOG.info(String
                            .format("由于您配置了writeMode append, 写入前不做清理工作, 数据写入Bucket [%s] 下, 写入相应Object的前缀为  [%s]",
                                    bucket, object));
                } else if ("nonConflict".equals(writeMode)) {
                    LOG.info(String
                            .format("由于您配置了writeMode nonConflict, 开始检查Bucket [%s] 下面以 [%s] 命名开头的Object",
                                    bucket, object));
                    ObjectListing listing = this.ossClient.listObjects(bucket,
                            object);
                    if (0 < listing.getObjectSummaries().size()) {
                        StringBuilder objectKeys = new StringBuilder();
                        objectKeys.append("[ ");
                        for (OSSObjectSummary ossObjectSummary : listing
                                .getObjectSummaries()) {
                            objectKeys.append(ossObjectSummary.getKey() + " ,");
                        }
                        objectKeys.append(" ]");
                        LOG.info(String.format(
                                "object with prefix [%s] details: %s", object,
                                objectKeys.toString()));
                        throw DataXException
                                .asDataXException(
                                        OssWriterErrorCode.ILLEGAL_VALUE,
                                        String.format(
                                                "您配置的Bucket: [%s] 下面存在其Object有前缀 [%s].",
                                                bucket, object));
                    }
                }
            } catch (OSSException e) {
                throw DataXException.asDataXException(
                        OssWriterErrorCode.OSS_COMM_ERROR, e.getMessage());
            } catch (ClientException e) {
                throw DataXException.asDataXException(
                        OssWriterErrorCode.OSS_COMM_ERROR, e.getMessage());
            }
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            LOG.info("begin do split...");
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            String object = this.writerSliceConfig.getString(Key.OBJECT);
            String bucket = this.writerSliceConfig.getString(Key.BUCKET);

            Set<String> allObjects = new HashSet<String>();
            try {
                List<OSSObjectSummary> ossObjectlisting = this.ossClient
                        .listObjects(bucket).getObjectSummaries();
                for (OSSObjectSummary objectSummary : ossObjectlisting) {
                    allObjects.add(objectSummary.getKey());
                }
            } catch (OSSException e) {
                throw DataXException.asDataXException(
                        OssWriterErrorCode.OSS_COMM_ERROR, e.getMessage());
            } catch (ClientException e) {
                throw DataXException.asDataXException(
                        OssWriterErrorCode.OSS_COMM_ERROR, e.getMessage());
            }

            String objectSuffix;
            for (int i = 0; i < mandatoryNumber; i++) {
                // handle same object name
                Configuration splitedTaskConfig = this.writerSliceConfig
                        .clone();

                String fullObjectName = null;
                objectSuffix = StringUtils.replace(
                        UUID.randomUUID().toString(), "-", "");
                fullObjectName = String.format("%s__%s", object, objectSuffix);
                while (allObjects.contains(fullObjectName)) {
                    objectSuffix = StringUtils.replace(UUID.randomUUID()
                            .toString(), "-", "");
                    fullObjectName = String.format("%s__%s", object,
                            objectSuffix);
                }
                allObjects.add(fullObjectName);

                splitedTaskConfig.set(Key.OBJECT, fullObjectName);

                LOG.info(String.format("splited write object name:[%s]",
                        fullObjectName));

                writerSplitConfigs.add(splitedTaskConfig);
            }
            LOG.info("end do split.");
            return writerSplitConfigs;
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private OSSClient ossClient;
        private Configuration writerSliceConfig;
        private String bucket;
        private String object;
        private String nullFormat;
        private String encoding;
        private char fieldDelimiter;
        private String dateFormat;
        private DateFormat dateParse;
        private String fileFormat;
        private List<String> header;
        private Long maxFileSize;// MB
        private String suffix;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.ossClient = OssUtil.initOssClient(this.writerSliceConfig);
            this.bucket = this.writerSliceConfig.getString(Key.BUCKET);
            this.object = this.writerSliceConfig.getString(Key.OBJECT);
            this.nullFormat = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.NULL_FORMAT);
            this.dateFormat = this.writerSliceConfig
                    .getString(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Key.DATE_FORMAT,
                            null);
            if (StringUtils.isNotBlank(this.dateFormat)) {
                this.dateParse = new SimpleDateFormat(dateFormat);
            }
            this.encoding = this.writerSliceConfig
                    .getString(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Key.ENCODING,
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.DEFAULT_ENCODING);
            this.fieldDelimiter = this.writerSliceConfig
                    .getChar(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FIELD_DELIMITER,
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.DEFAULT_FIELD_DELIMITER);
            this.fileFormat = this.writerSliceConfig
                    .getString(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_FORMAT,
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.FILE_FORMAT_TEXT);
            this.header = this.writerSliceConfig
                    .getList(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Key.HEADER,
                            null, String.class);
            this.maxFileSize = this.writerSliceConfig
                    .getLong(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Key.MAX_FILE_SIZE,
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.MAX_FILE_SIZE);
            this.suffix = this.writerSliceConfig
                    .getString(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Key.SUFFIX,
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.DEFAULT_SUFFIX);
            this.suffix = this.suffix.trim();// warn: need trim
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            // 设置每块字符串长度
            final long partSize = 1024 * 1024 * 10L;
            long numberCacul = (this.maxFileSize * 1024 * 1024L) / partSize;
            final long maxPartNumber = numberCacul >= 1 ? numberCacul : 1;
            int objectRollingNumber = 0;
            //warn: may be StringBuffer->StringBuilder
            StringWriter sw = new StringWriter();
            StringBuffer sb = sw.getBuffer();
            UnstructuredWriter unstructuredWriter = TextCsvWriterManager
                    .produceUnstructuredWriter(this.fileFormat,
                            this.fieldDelimiter, sw);
            Record record = null;

            LOG.info(String.format(
                    "begin do write, each object maxFileSize: [%s]MB...",
                    maxPartNumber * 10));
            String currentObject = this.object;
            InitiateMultipartUploadRequest currentInitiateMultipartUploadRequest = null;
            InitiateMultipartUploadResult currentInitiateMultipartUploadResult = null;
            boolean gotData = false;
            List<PartETag> currentPartETags = null;
            // to do:
            // 可以根据currentPartNumber做分块级别的重试，InitiateMultipartUploadRequest多次一个currentPartNumber会覆盖原有
            int currentPartNumber = 1;
            try {
                // warn
                boolean needInitMultipartTransform = true;
                while ((record = lineReceiver.getFromReader()) != null) {
                    gotData = true;
                    // init:begin new multipart upload
                    if (needInitMultipartTransform) {
                        if (objectRollingNumber == 0) {
                            if (StringUtils.isBlank(this.suffix)) {
                                currentObject = this.object;
                            } else {
                                currentObject = String.format("%s%s",
                                        this.object, this.suffix);
                            }
                        } else {
                            // currentObject is like(no suffix)
                            // myfile__9b886b70fbef11e59a3600163e00068c_1
                            if (StringUtils.isBlank(this.suffix)) {
                                currentObject = String.format("%s_%s",
                                        this.object, objectRollingNumber);
                            } else {
                                // or with suffix
                                // myfile__9b886b70fbef11e59a3600163e00068c_1.csv
                                currentObject = String.format("%s_%s%s",
                                        this.object, objectRollingNumber,
                                        this.suffix);
                            }
                        }
                        objectRollingNumber++;
                        currentInitiateMultipartUploadRequest = new InitiateMultipartUploadRequest(
                                this.bucket, currentObject);
                        currentInitiateMultipartUploadResult = this.ossClient
                                .initiateMultipartUpload(currentInitiateMultipartUploadRequest);
                        currentPartETags = new ArrayList<PartETag>();
                        LOG.info(String
                                .format("write to bucket: [%s] object: [%s] with oss uploadId: [%s]",
                                        this.bucket, currentObject,
                                        currentInitiateMultipartUploadResult
                                                .getUploadId()));

                        // each object's header
                        if (null != this.header && !this.header.isEmpty()) {
                            unstructuredWriter.writeOneRecord(this.header);
                        }
                        // warn
                        needInitMultipartTransform = false;
                        currentPartNumber = 1;
                    }

                    // write: upload data to current object
                    UnstructuredStorageWriterUtil.transportOneRecord(record,
                            this.nullFormat, this.dateParse,
                            this.getTaskPluginCollector(), unstructuredWriter);

                    if (sb.length() >= partSize) {
                        this.uploadOnePart(sw, currentPartNumber,
                                currentInitiateMultipartUploadResult,
                                currentPartETags, currentObject);
                        currentPartNumber++;
                        sb.setLength(0);
                    }

                    // save: end current multipart upload
                    if (currentPartNumber > maxPartNumber) {
                        LOG.info(String
                                .format("current object [%s] size > %s, complete current multipart upload and begin new one",
                                        currentObject, currentPartNumber
                                                * partSize));
                        CompleteMultipartUploadRequest currentCompleteMultipartUploadRequest = new CompleteMultipartUploadRequest(
                                this.bucket, currentObject,
                                currentInitiateMultipartUploadResult
                                        .getUploadId(), currentPartETags);
                        CompleteMultipartUploadResult currentCompleteMultipartUploadResult = this.ossClient
                                .completeMultipartUpload(currentCompleteMultipartUploadRequest);
                        LOG.info(String.format(
                                "final object [%s] etag is:[%s]",
                                currentObject,
                                currentCompleteMultipartUploadResult.getETag()));
                        // warn
                        needInitMultipartTransform = true;
                    }
                }

                if (!gotData) {
                    LOG.info("Receive no data from the source.");
                    currentInitiateMultipartUploadRequest = new InitiateMultipartUploadRequest(
                            this.bucket, currentObject);
                    currentInitiateMultipartUploadResult = this.ossClient
                            .initiateMultipartUpload(currentInitiateMultipartUploadRequest);
                    currentPartETags = new ArrayList<PartETag>();
                    // each object's header
                    if (null != this.header && !this.header.isEmpty()) {
                        unstructuredWriter.writeOneRecord(this.header);
                    }
                }
                // warn: may be some data stall in sb
                if (0 < sb.length()) {
                    this.uploadOnePart(sw, currentPartNumber,
                            currentInitiateMultipartUploadResult,
                            currentPartETags, currentObject);
                }
                CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest(
                        this.bucket, currentObject,
                        currentInitiateMultipartUploadResult.getUploadId(),
                        currentPartETags);
                CompleteMultipartUploadResult completeMultipartUploadResult = this.ossClient
                        .completeMultipartUpload(completeMultipartUploadRequest);
                LOG.info(String.format("final object etag is:[%s]",
                        completeMultipartUploadResult.getETag()));
            } catch (IOException e) {
                // 脏数据UnstructuredStorageWriterUtil.transportOneRecord已经记录,header
                // 都是字符串不认为有脏数据
                throw DataXException.asDataXException(
                        OssWriterErrorCode.Write_OBJECT_ERROR, e.getMessage());
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        OssWriterErrorCode.Write_OBJECT_ERROR, e.getMessage());
            }
            LOG.info("end do write");
        }

        /**
         * 对于同一个UploadID，该号码不但唯一标识这一块数据，也标识了这块数据在整个文件内的相对位置。
         * 如果你用同一个part号码，上传了新的数据，那么OSS上已有的这个号码的Part数据将被覆盖。
         * 
         * @throws Exception
         * */
        private void uploadOnePart(
                final StringWriter sw,
                final int partNumber,
                final InitiateMultipartUploadResult initiateMultipartUploadResult,
                final List<PartETag> partETags, final String currentObject)
                throws Exception {
            final String encoding = this.encoding;
            final String bucket = this.bucket;
            final OSSClient ossClient = this.ossClient;
            RetryUtil.executeWithRetry(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    byte[] byteArray = sw.toString().getBytes(encoding);
                    InputStream inputStream = new ByteArrayInputStream(
                            byteArray);
                    // 创建UploadPartRequest，上传分块
                    UploadPartRequest uploadPartRequest = new UploadPartRequest();
                    uploadPartRequest.setBucketName(bucket);
                    uploadPartRequest.setKey(currentObject);
                    uploadPartRequest.setUploadId(initiateMultipartUploadResult
                            .getUploadId());
                    uploadPartRequest.setInputStream(inputStream);
                    uploadPartRequest.setPartSize(byteArray.length);
                    uploadPartRequest.setPartNumber(partNumber);
                    UploadPartResult uploadPartResult = ossClient
                            .uploadPart(uploadPartRequest);
                    partETags.add(uploadPartResult.getPartETag());
                    LOG.info(String
                            .format("upload part [%s] size [%s] Byte has been completed.",
                                    partNumber, byteArray.length));
                    IOUtils.closeQuietly(inputStream);
                    return true;
                }
            }, 3, 1000L, false);
        }

        @Override
        public void prepare() {

        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }
    }
}
