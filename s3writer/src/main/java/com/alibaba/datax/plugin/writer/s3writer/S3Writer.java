package com.alibaba.datax.plugin.writer.s3writer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.unstructuredstorage.writer.TextCsvWriterManager;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredStorageWriterUtil;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredWriter;
import com.alibaba.datax.plugin.writer.s3writer.util.S3ClientUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.UUID;

/**
 * S3 writer
 *
 * @author mengxin.liumx
 * @author L.cm
 */
public class S3Writer extends Writer {
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration writerSliceConfig = null;
        private S3Client s3Client = null;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.validateParameter();
            this.s3Client = S3ClientUtil.initOssClient(this.writerSliceConfig);
        }

        private void validateParameter() {
            this.writerSliceConfig.getNecessaryValue(Key.ACCESSKEY, S3WriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(Key.SECRETKEY, S3WriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(Key.REGION, S3WriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(Key.BUCKET, S3WriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(Key.OBJECT, S3WriterErrorCode.REQUIRED_VALUE);
            // warn: do not support compress!!
            String compress = this.writerSliceConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.COMPRESS);
            if (StringUtils.isNotBlank(compress)) {
                String errorMessage = String.format("S3写暂时不支持压缩, 该压缩配置项[%s]不起效用", compress);
                LOG.error(errorMessage);
                throw DataXException.asDataXException(S3WriterErrorCode.ILLEGAL_VALUE, errorMessage);

            }
            UnstructuredStorageWriterUtil.validateParameter(this.writerSliceConfig);
        }

        @Override
        public void prepare() {
            LOG.info("begin do prepare...");
            String bucket = this.writerSliceConfig.getString(Key.BUCKET);
            String object = this.writerSliceConfig.getString(Key.OBJECT);
            String writeMode = this.writerSliceConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.WRITE_MODE);
            // warn: bucket is not exists, create it
            // warn: do not create bucket for user
            if (!S3ClientUtil.doesBucketExist(s3Client, bucket)) {
                String errorMessage = String.format("您配置的bucket [%s] 不存在, 请您确认您的配置项.", bucket);
                LOG.error(errorMessage);
                throw DataXException.asDataXException(S3WriterErrorCode.ILLEGAL_VALUE, errorMessage);
            }
            LOG.info(String.format("access control details [%s].", S3ClientUtil.getBucketAcl(s3Client, bucket)));
            // truncate option handler
            if ("truncate".equals(writeMode)) {
                LOG.info(String.format("由于您配置了writeMode truncate, 开始清理 [%s] 下面以 [%s] 开头的Object", bucket, object));
                // warn: 默认情况下，如果Bucket中的Object数量大于100，则只会返回100个Object
                while (true) {
                    LOG.info("list objects with listObject(bucket, object)");
                    List<String> keyList = S3ClientUtil.listObjects(s3Client, bucket, object);
                    if (keyList.isEmpty()) {
                        break;
                    }
                    S3ClientUtil.deleteObjects(s3Client, bucket, keyList);
                }
            } else if ("append".equals(writeMode)) {
                LOG.info(String.format("由于您配置了writeMode append, 写入前不做清理工作, 数据写入Bucket [%s] 下, 写入相应Object的前缀为  [%s]", bucket, object));
            } else if ("nonConflict".equals(writeMode)) {
                LOG.info(String.format("由于您配置了writeMode nonConflict, 开始检查Bucket [%s] 下面以 [%s] 命名开头的Object", bucket, object));
                List<String> listObjects = S3ClientUtil.listObjects(s3Client, bucket, object);
                if (0 < listObjects.size()) {
                    StringJoiner joiner = new StringJoiner("[ ", " ,", " ]");
                    for (String objectKey : listObjects) {
                        joiner.add(objectKey);
                    }
                    LOG.info(String.format(
                            "object with prefix [%s] details: %s", object, joiner.toString()));
                    throw DataXException.asDataXException(S3WriterErrorCode.ILLEGAL_VALUE,
                            String.format("您配置的Bucket: [%s] 下面存在其Object有前缀 [%s].", bucket, object));
                }
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
            List<Configuration> writerSplitConfigs = new ArrayList<>();
            String object = this.writerSliceConfig.getString(Key.OBJECT);
            String bucket = this.writerSliceConfig.getString(Key.BUCKET);
            // 注意：只列出 100 个对象
            List<String> allObjects = S3ClientUtil.listObjects(s3Client, bucket);
            String objectSuffix;
            for (int i = 0; i < mandatoryNumber; i++) {
                // handle same object name
                Configuration splitedTaskConfig = this.writerSliceConfig.clone();

                String fullObjectName = null;
                objectSuffix = StringUtils.replace(UUID.randomUUID().toString(), "-", "");
                fullObjectName = String.format("%s__%s", object, objectSuffix);
                while (allObjects.contains(fullObjectName)) {
                    objectSuffix = StringUtils.replace(UUID.randomUUID().toString(), "-", "");
                    fullObjectName = String.format("%s__%s", object, objectSuffix);
                }
                allObjects.add(fullObjectName);
                splitedTaskConfig.set(Key.OBJECT, fullObjectName);
                LOG.info(String.format("splited write object name:[%s]", fullObjectName));
                writerSplitConfigs.add(splitedTaskConfig);
            }
            LOG.info("end do split.");
            return writerSplitConfigs;
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private S3Client s3Client;
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
            this.s3Client = S3ClientUtil.initOssClient(this.writerSliceConfig);
            this.bucket = this.writerSliceConfig.getString(Key.BUCKET);
            this.object = this.writerSliceConfig.getString(Key.OBJECT);
            this.nullFormat = this.writerSliceConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.NULL_FORMAT);
            this.dateFormat = this.writerSliceConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.DATE_FORMAT, null);
            if (StringUtils.isNotBlank(this.dateFormat)) {
                this.dateParse = new SimpleDateFormat(dateFormat);
            }
            this.encoding = this.writerSliceConfig.getString(
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Key.ENCODING,
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.DEFAULT_ENCODING);
            this.fieldDelimiter = this.writerSliceConfig.getChar(
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FIELD_DELIMITER,
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.DEFAULT_FIELD_DELIMITER);
            this.fileFormat = this.writerSliceConfig.getString(
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_FORMAT,
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.FILE_FORMAT_TEXT);
            this.header = this.writerSliceConfig.getList(
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Key.HEADER,
                    null, String.class);
            this.maxFileSize = this.writerSliceConfig.getLong(
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Key.MAX_FILE_SIZE,
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.MAX_FILE_SIZE);
            this.suffix = this.writerSliceConfig.getString(
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
            UnstructuredWriter unstructuredWriter = TextCsvWriterManager.produceUnstructuredWriter(this.fileFormat, this.fieldDelimiter, sw);
            Record record = null;

            LOG.info(String.format("begin do write, each object maxFileSize: [%s]MB...", maxPartNumber * 10));

            String currentObject = this.object;

            CompleteMultipartUploadRequest multipartUploadRequest = null;
            String currentInitiateMultipartUploadId = null;
            boolean gotData = false;
            List<CompletedPart> currentPartETags = null;
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
                                currentObject = String.format("%s%s", this.object, this.suffix);
                            }
                        } else {
                            // currentObject is like(no suffix)
                            // myfile__9b886b70fbef11e59a3600163e00068c_1
                            if (StringUtils.isBlank(this.suffix)) {
                                currentObject = String.format("%s_%s", this.object, objectRollingNumber);
                            } else {
                                // or with suffix
                                // myfile__9b886b70fbef11e59a3600163e00068c_1.csv
                                currentObject = String.format("%s_%s%s", this.object, objectRollingNumber, this.suffix);
                            }
                        }
                        objectRollingNumber++;
                        currentInitiateMultipartUploadId = S3ClientUtil.createMultipartUpload(s3Client, bucket, currentObject);
                        currentPartETags = new ArrayList<>();
                        LOG.info(String.format("write to bucket: [%s] object: [%s] with oss uploadId: [%s]",
                                this.bucket, currentObject, currentInitiateMultipartUploadId));

                        // each object's header
                        if (null != this.header && !this.header.isEmpty()) {
                            unstructuredWriter.writeOneRecord(this.header);
                        }
                        // warn
                        needInitMultipartTransform = false;
                        currentPartNumber = 1;
                    }

                    // write: upload data to current object
                    UnstructuredStorageWriterUtil.transportOneRecord(record, this.nullFormat, this.dateParse,
                            this.getTaskPluginCollector(), unstructuredWriter);

                    if (sb.length() >= partSize) {
                        this.uploadOnePart(sw, currentPartNumber, currentInitiateMultipartUploadId, currentPartETags, currentObject);
                        currentPartNumber++;
                        sb.setLength(0);
                    }

                    // save: end current multipart upload
                    if (currentPartNumber > maxPartNumber) {
                        LOG.info(String.format("current object [%s] size > %s, complete current multipart upload and begin new one",
                                currentObject, currentPartNumber * partSize));

                        CompleteMultipartUploadResponse response = S3ClientUtil.completeMultipartUpload(s3Client, bucket, currentObject,
                                currentInitiateMultipartUploadId, currentPartETags);
                        LOG.info(String.format("final object [%s] etag is:[%s]", currentObject, response.eTag()));
                        // warn
                        needInitMultipartTransform = true;
                    }
                }

                if (!gotData) {
                    LOG.info("Receive no data from the source.");
                    currentInitiateMultipartUploadId = S3ClientUtil.createMultipartUpload(s3Client, bucket, currentObject);
                    currentPartETags = new ArrayList<>();
                    // each object's header
                    if (null != this.header && !this.header.isEmpty()) {
                        unstructuredWriter.writeOneRecord(this.header);
                    }
                }
                // warn: may be some data stall in sb
                if (0 < sb.length()) {
                    this.uploadOnePart(sw, currentPartNumber, currentInitiateMultipartUploadId,
                            currentPartETags, currentObject);
                }
                CompleteMultipartUploadResponse response = S3ClientUtil.completeMultipartUpload(s3Client, bucket, currentObject,
                        currentInitiateMultipartUploadId, currentPartETags);
                LOG.info(String.format("final object etag is:[%s]", response.eTag()));
            } catch (Exception e) {
                // 脏数据UnstructuredStorageWriterUtil.transportOneRecord已经记录,header
                // 都是字符串不认为有脏数据
                throw DataXException.asDataXException(S3WriterErrorCode.Write_OBJECT_ERROR, e.getMessage());
            }
            LOG.info("end do write");
        }

        /**
         * 对于同一个UploadID，该号码不但唯一标识这一块数据，也标识了这块数据在整个文件内的相对位置。
         * 如果你用同一个part号码，上传了新的数据，那么OSS上已有的这个号码的Part数据将被覆盖。
         *
         * @throws Exception
         */
        private void uploadOnePart(
                final StringWriter sw,
                final int partNumber,
                final String uploadId,
                final List<CompletedPart> partETags, final String currentObject)
                throws Exception {
            final String encoding = this.encoding;
            final String bucket = this.bucket;
            final S3Client s3Client = this.s3Client;
            RetryUtil.executeWithRetry(() -> {
                byte[] byteArray = sw.toString().getBytes(encoding);
                InputStream inputStream = new ByteArrayInputStream(byteArray);
                // 创建UploadPartRequest，上传分块
                UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                        .bucket(bucket)
                        .key(currentObject)
                        .uploadId(uploadId)
                        .partNumber(partNumber)
                        .build();
                RequestBody requestBody = RequestBody.fromInputStream(inputStream, byteArray.length);
                UploadPartResponse uploadPartResult = S3ClientUtil.uploadPart(s3Client, uploadPartRequest, requestBody);
                CompletedPart completedPart = CompletedPart.builder()
                        .partNumber(partNumber)
                        .eTag(uploadPartResult.eTag())
                        .build();
                partETags.add(completedPart);
                LOG.info(String.format("upload part [%s] size [%s] Byte has been completed.", partNumber, byteArray.length));
                IOUtils.closeQuietly(inputStream);
                return true;
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
