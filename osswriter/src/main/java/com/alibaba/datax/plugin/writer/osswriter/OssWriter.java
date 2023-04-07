package com.alibaba.datax.plugin.writer.osswriter;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;

import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.util.RangeSplitUtil;
import com.alibaba.datax.plugin.unstructuredstorage.FileFormat;
import com.alibaba.datax.plugin.unstructuredstorage.writer.binaryFileUtil.BinaryFileWriterUtil;
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsWriter;
import com.alibaba.datax.plugin.writer.osswriter.util.HandlerUtil;
import com.alibaba.datax.plugin.writer.osswriter.util.HdfsParquetUtil;
import com.alibaba.fastjson2.JSON;
import com.aliyun.oss.model.*;
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

import static com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.*;

/**
 * Created by haiwei.luo on 15-02-09.
 */
public class OssWriter extends Writer {

    public static int parseParentPathLength(List<String> path) {
        if (path == null || path.size() != 1) {
            throw DataXException.asDataXException(
                    OssWriterErrorCode.CONFIG_INVALID_EXCEPTION, String.format("only support configure one path in binary copy mode, your config: %s", JSON.toJSONString(path)));
        }
        String eachPath = path.get(0);
        int endMark;
        for (endMark = 0; endMark < eachPath.length(); endMark++) {
            if ('*' != eachPath.charAt(endMark) && '?' != eachPath.charAt(endMark)) {
                continue;
            } else {
                break;
            }
        }

        int lastDirSeparator = eachPath.lastIndexOf(IOUtils.DIR_SEPARATOR) + 1;
        if (endMark < eachPath.length()) {
            lastDirSeparator = eachPath.substring(0, endMark).lastIndexOf(IOUtils.DIR_SEPARATOR) + 1;
        }
        return lastDirSeparator;
    }

    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration writerSliceConfig = null;
        private OSSClient ossClient = null;

        private Configuration peerPluginJobConf;
        private Boolean isBinaryFile;
        private String objectDir;
        private String syncMode;
        private String fileFormat;
        private String encoding;
        private HdfsWriter.Job hdfsWriterJob;
        private boolean useHdfsWriterProxy = false;
        private boolean writeSingleObject;
        private OssWriterProxy ossWriterProxy;
        private String bucket;
        private String object;
        private List<String> header;

        @Override
        public void preHandler(Configuration jobConfiguration) {
            HandlerUtil.preHandler(jobConfiguration);
        }

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.basicValidateParameter();
            this.fileFormat = this.writerSliceConfig.getString(
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_FORMAT,
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.FILE_FORMAT_TEXT);
            this.encoding = this.writerSliceConfig.getString(
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Key.ENCODING,
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.DEFAULT_ENCODING);
            this.useHdfsWriterProxy  = HdfsParquetUtil.isUseHdfsWriterProxy(this.fileFormat);
            if(useHdfsWriterProxy){
                this.hdfsWriterJob = new HdfsWriter.Job();
                HdfsParquetUtil.adaptConfiguration(this.hdfsWriterJob, this.writerSliceConfig);

                this.hdfsWriterJob.setJobPluginCollector(this.getJobPluginCollector());
                this.hdfsWriterJob.setPeerPluginJobConf(this.getPeerPluginJobConf());
                this.hdfsWriterJob.setPeerPluginName(this.getPeerPluginName());
                this.hdfsWriterJob.setPluginJobConf(this.getPluginJobConf());
                this.hdfsWriterJob.init();
                return;
            }
            this.peerPluginJobConf = this.getPeerPluginJobConf();
            this.isBinaryFile = FileFormat.getFileFormatByConfiguration(this.peerPluginJobConf).isBinary();
            this.syncMode = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.SYNC_MODE, "");
            this.writeSingleObject = this.writerSliceConfig.getBool(Key.WRITE_SINGLE_OBJECT, false);
            this.header = this.writerSliceConfig
                    .getList(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.HEADER, null, String.class);
            this.validateParameter();
            this.ossClient = OssUtil.initOssClient(this.writerSliceConfig);
            this.ossWriterProxy = new OssWriterProxy(this.writerSliceConfig, this.ossClient);
        }

        private void basicValidateParameter(){
            this.writerSliceConfig.getNecessaryValue(Key.ENDPOINT, OssWriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(Key.ACCESSID, OssWriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(Key.ACCESSKEY, OssWriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(Key.BUCKET, OssWriterErrorCode.REQUIRED_VALUE);
        }

        private void validateParameter() {
            this.writerSliceConfig.getBool(Key.ENCRYPT);

            if (this.isBinaryFile){
                BinaryFileWriterUtil.validateParameter(this.writerSliceConfig);
                return;
            }

            if (!this.isPeer2PeerCopyMode()) {
                // 非对等拷贝模式下必选
                this.writerSliceConfig.getNecessaryValue(Key.OBJECT,
                        OssWriterErrorCode.REQUIRED_VALUE);
            }

            // warn: do not support compress!!
            String compress = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.COMPRESS);
            if (StringUtils.isNotBlank(compress)) {
                String errorMessage = String.format("OSS writes do not support compression for the moment. The compressed item %s does not work", compress);
                LOG.error(errorMessage);
                throw DataXException.asDataXException(
                        OssWriterErrorCode.ILLEGAL_VALUE, errorMessage);

            }
            UnstructuredStorageWriterUtil
                    .validateParameter(this.writerSliceConfig);
            LOG.info("writeSingleObject is: {}", this.writeSingleObject);
        }

        @Override
        public void prepare() {
            LOG.info("begin do prepare...");
            if(useHdfsWriterProxy){
                this.hdfsWriterJob.prepare();
                return;
            }
            this.bucket = this.writerSliceConfig.getString(Key.BUCKET);
            this.object = this.writerSliceConfig.getString(Key.OBJECT);
            String writeMode = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.WRITE_MODE);
            List<String> sourceFileName = this.peerPluginJobConf.getList(SOURCE_FILE_NAME, new ArrayList<String>(),
                    String.class);
            this.objectDir = this.getObjectDir(object);

            // 对等拷贝模式下将源头获取的文件列表在目的端删除
            if (this.isPeer2PeerCopyMode()) {
                String fullObjectName = null;
                String truncateMode = this.writerSliceConfig.getString("truncateMode", "objectMatch");
                // 前缀删除模式
                if ("prefix".equalsIgnoreCase(truncateMode)) {
                    BinaryFileWriterUtil.checkFileNameIfRepeatedThrowException(sourceFileName);
                    if (TRUNCATE.equals(writeMode)) {
                        LOG.info("You have configured [writeMode] [truncate], so the system will start to clear the objects starting with [{}] under [{}]. ", bucket, object);
                        // warn: 默认情况下，如果Bucket中的Object数量大于100，则只会返回100个Object
                        while (true) {
                            ObjectListing listing = null;
                            LOG.info("list objects with listObject(bucket, object)");
                            listing = this.ossClient.listObjects(bucket, object);
                            List<OSSObjectSummary> objectSummarys = listing
                                    .getObjectSummaries();
                            if (objectSummarys.isEmpty()) {
                                break;
                            }
                            List<String> objects2Delete = new ArrayList<String>();
                            for (OSSObjectSummary objectSummary : objectSummarys) {
                                objects2Delete.add(objectSummary.getKey());
                            }
                            LOG.info(String.format("[prefix truncate mode]delete oss object [%s].", JSON.toJSONString(objects2Delete)));
                            DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucket);
                            deleteRequest.setKeys(objects2Delete);
                            deleteRequest.setQuiet(true);// 简单模式
                            DeleteObjectsResult deleteResult = this.ossClient.deleteObjects(deleteRequest);
                            assert deleteResult.getDeletedObjects().isEmpty();
                            LOG.warn("OSS request id:{}, objects delete failed:{}", deleteResult.getRequestId(),
                                    JSON.toJSONString(deleteResult.getDeletedObjects()));
                        }

                    }else {
                        throw DataXException.asDataXException(OssWriterErrorCode.ILLEGAL_VALUE,
                                "only support truncate writeMode in copy sync mode.");
                    }
                } else {
                    if (TRUNCATE.equals(writeMode)) {
                        sourceFileName = this.peerPluginJobConf.getList(com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.SOURCE_FILE, new ArrayList<String>(),
                                String.class);
                        List<String> readerPath =  this.peerPluginJobConf.getList(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.PATH, new ArrayList<String>(),
                                String.class);
                        int parentPathLength = OssWriter.parseParentPathLength(readerPath);
                        this.writerSliceConfig.set("__parentPathLength", parentPathLength);
                        BinaryFileWriterUtil.checkFileNameIfRepeatedThrowException(sourceFileName);

                        // 原样文件名删除模式
                        int splitCount = sourceFileName.size() / 1000 + 1;
                        List<List<String>> splitResult = RangeSplitUtil.doListSplit(sourceFileName, splitCount);
                        for (List<String> eachSlice : splitResult) {
                            assert eachSlice.size() <= 1000;
                            if (eachSlice.isEmpty()) {
                                continue;
                            }
                            List<String> ossObjFullPath = new ArrayList<String>();
                            for (String eachObj : eachSlice) {
                                fullObjectName = String.format("%s%s", objectDir, eachObj.substring(parentPathLength, eachObj.length()));
                                ossObjFullPath.add(fullObjectName);
                            }
                            LOG.info(String.format("[origin object name truncate mode]delete oss object [%s].", JSON.toJSONString(ossObjFullPath)));
                            DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucket);
                            deleteRequest.setKeys(ossObjFullPath);
                            deleteRequest.setQuiet(true);// 简单模式
                            DeleteObjectsResult deleteResult = this.ossClient.deleteObjects(deleteRequest);
                            assert deleteResult.getDeletedObjects().isEmpty();
                            LOG.warn("OSS request id:{}, objects delete failed:{}", deleteResult.getRequestId(),
                                    JSON.toJSONString(deleteResult.getDeletedObjects()));
                        }
                    } else {
                        throw DataXException.asDataXException(OssWriterErrorCode.ILLEGAL_VALUE,
                                "only support truncate writeMode in copy sync mode.");
                    }
                }
                return;
            } else {
                // warn: 源头表不是半结构化或者不是对等copy模式时走前缀删除策略
                // warn: bucket is not exists, create it
                try {
                    // warn: do not create bucket for user
                    if (!this.ossClient.doesBucketExist(bucket)) {
                        // this.ossClient.createBucket(bucket);
                        String errorMessage = String.format("The [bucket]: %s you configured does not exist. Please confirm your configuration items. ", bucket);
                        LOG.error(errorMessage);
                        throw DataXException.asDataXException(
                                OssWriterErrorCode.ILLEGAL_VALUE, errorMessage);
                    }
                    LOG.info(String.format("access control details [%s].",
                            this.ossClient.getBucketAcl(bucket).toString()));

                    if (writeSingleObject) {
                        doPrepareForSingleObject(bucket, object, writeMode);
                    } else {
                        doPrepareForMutliObject(bucket, object, writeMode);
                    }
                } catch (OSSException e) {
                    throw DataXException.asDataXException(
                            OssWriterErrorCode.OSS_COMM_ERROR, e.getMessage(), e);
                } catch (ClientException e) {
                    throw DataXException.asDataXException(
                            OssWriterErrorCode.OSS_COMM_ERROR, e.getMessage(), e);
                }
            }
        }

        /**
         * 执行多个task写单个object prepare逻辑
         *
         * @param bucket
         * @param object
         * @param writeMode
         */
        private void doPrepareForSingleObject(String bucket, String object, String writeMode) {
            boolean doesObjectExist = this.ossClient.doesObjectExist(bucket, object);
            LOG.info("does object [{}] exist in bucket {} : {}", object, bucket, doesObjectExist);
            if (TRUNCATE.equals(writeMode)) {
                LOG.info("Because you have configured writeMode truncate, and writeSingleObject is true, start cleaning up the duplicate object [{}] under [{}]", bucket, object);
                if (doesObjectExist) {
                    LOG.info("object [{}] has exist in bucket, delete it!", object, bucket);
                    this.ossClient.deleteObject(bucket, object);
                }
            } else if (APPEND.equals(writeMode)) {
                throw DataXException
                        .asDataXException(
                                OssWriterErrorCode.ILLEGAL_VALUE,
                                "Illegal value");
            } else if (NOCONFLICT.equals(writeMode)) {
                LOG.info("Because you have configured writeMode nonConflict, and writeSingleObject is true, start checking bucket [{}] under the same name object [{}]", bucket, object);
                if (doesObjectExist) {
                    throw DataXException
                            .asDataXException(
                                    OssWriterErrorCode.ILLEGAL_VALUE,
                                    String.format("Buffet you configured: %s There is a duplicate name of Object %s", bucket, object));
                }
            }
        }

        /**
         * 执行多个task写多个object的prepare逻辑，这个是osswriter已有的逻辑，需要保持向前兼容性
         *
         * @param bucket
         * @param object
         * @param writeMode
         */
        private void doPrepareForMutliObject(String bucket, String object, String writeMode) {
            // truncate option handler
            if (TRUNCATE.equals(writeMode)) {
                LOG.info("You have configured [writeMode] [truncate], so the system will start to clear the objects starting with [{}] under [{}]. ", bucket, object);
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
            } else if (APPEND.equals(writeMode)) {
                LOG.info("You have configured [writeMode] [append], so the system won\\u2019t perform the clearing before writing. Data is written to objects with the name prefix of [{}] under the bucket: [{}]. ", bucket, object);
            } else if (NOCONFLICT.equals(writeMode)) {
                LOG.info("You have configured [writeMode] [nonConflict], so the system will start to check objects whose names start with [{}] under the bucket: [{}]. ", bucket, object);
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
                                    String.format("The [bucket] you configured: %s contains objects with the name prefix of %s.", bucket, object));
                }
            }
        }

        @Override
        public void post() {
            if(useHdfsWriterProxy){
                this.hdfsWriterJob.post();
                return;
            }
            if (this.writeSingleObject) {
                try {
                    /**1. 合并上传最后一个block*/
                    LOG.info("Has upload part size: {}", OssSingleObject.allPartETags.size());
                    if (OssSingleObject.getLastBlockBuffer() != null && OssSingleObject.getLastBlockBuffer().length != 0) {
                        byte[] byteBuffer = OssSingleObject.getLastBlockBuffer();
                        LOG.info("post writer single object last merge block size is : {}", byteBuffer.length);
                        this.ossWriterProxy.uploadOnePartForSingleObject(byteBuffer, OssSingleObject.uploadId,
                                OssSingleObject.allPartETags, this.object, this::getHeaderBytes);
                    }

                    if (OssSingleObject.allPartETags.size() == 0) {
                        LOG.warn("allPartETags size is 0, there is no part of data need to be complete uploaded, "
                                + "skip complete multipart upload!");
                        this.ossWriterProxy.abortMultipartUpload(this.object,OssSingleObject.uploadId);
                        return;
                    }

                    /**2. 完成complete upload */
                    LOG.info("begin complete multi part upload, bucket:{}, object:{}, uploadId:{}, all has upload part size:{}",
                            this.bucket, this.object, OssSingleObject.uploadId, OssSingleObject.allPartETags.size());
                    orderPartETages(OssSingleObject.allPartETags);
                    CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest(
                            this.bucket, this.object, OssSingleObject.uploadId, OssSingleObject.allPartETags);
                    CompleteMultipartUploadResult completeMultipartUploadResult = this.ossWriterProxy.completeMultipartUpload(completeMultipartUploadRequest);
                    LOG.info(String.format("post final object etag is:[%s]", completeMultipartUploadResult.getETag()));
                } catch (Exception e) {
                    LOG.error("osswriter post error: {}", e.getMessage(), e);
                    throw DataXException.asDataXException(e.getMessage());
                }
            }
        }

        private byte[] getHeaderBytes() throws IOException {
            if (null != this.header && !this.header.isEmpty()) {
                // write header to writer
                try (StringWriter sw = new StringWriter();
                     UnstructuredWriter headerWriter = UnstructuredStorageWriterUtil.
                             produceUnstructuredWriter(this.fileFormat, this.writerSliceConfig, sw)) {
                    headerWriter.writeOneRecord(this.header);
                    return sw.toString().getBytes(this.encoding);
                }
            }
            return new byte[0];
        }

        /**
         * 对allPartETags做递增排序
         *
         * @param allPartETags
         * @return
         */
        private void orderPartETages(List<PartETag> allPartETags) {
            Collections.sort(allPartETags, new Comparator<PartETag>() {
                @Override
                public int compare(PartETag o1, PartETag o2) {
                    //按照partNumber递增排序
                    return o1.getPartNumber() - o2.getPartNumber();
                }
            });
        }

        @Override
        public void destroy() {
            if(useHdfsWriterProxy){
                this.hdfsWriterJob.destroy();
                return;
            }
            try {
                // this.ossClient.shutdown();
            } catch (Exception e) {
                LOG.warn("shutdown ossclient meet a exception:" + e.getMessage(), e);
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            LOG.info("begin do split...");
            if(useHdfsWriterProxy){
                return this.hdfsWriterJob.split(mandatoryNumber);
            }
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();

            // warn: 这个地方其实可能有bug，datax frame其实会shuffle, 文件内部切分也不好支持这个诉求
            if(this.isPeer2PeerCopyMode()){
                // 有这个需求风险: 源头oss的文件 abc/123/data.txt yixiao.txt 2个文件对等拷贝过来， 这个场景下data.txt
                // yixiao.txt 只能放一个目录
                List<Configuration> readerSplitConfigs = this.getReaderPluginSplitConf();
                for (int i = 0; i < readerSplitConfigs.size(); i++) {
                    Configuration splitedTaskConfig = writerSliceConfig.clone();
                    splitedTaskConfig.set(Key.OBJECT, objectDir);
                    splitedTaskConfig.set(com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.BINARY,
                            this.isBinaryFile);
                    writerSplitConfigs.add(splitedTaskConfig);
                }
            } else {
                if (this.writeSingleObject) {
                    writerSplitConfigs = doSplitForWriteSingleObject(mandatoryNumber);
                } else {
                    writerSplitConfigs = doSplitForWriteMultiObject(mandatoryNumber);
                }
            }
            LOG.info("end do split. split size: {}", writerSplitConfigs.size());
            return writerSplitConfigs;
        }

        /**
         * 针对多个task写单个文件模式，新增split逻辑
         *
         * @param mandatoryNumber
         * @return
         */
        private List<Configuration> doSplitForWriteSingleObject(int mandatoryNumber) {
            LOG.info("writeSingleObject is true, begin do split for write single object.");
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            String object = this.writerSliceConfig.getString(Key.OBJECT);

            InitiateMultipartUploadRequest uploadRequest = this.ossWriterProxy.getInitiateMultipartUploadRequest(
                    object);

            InitiateMultipartUploadResult uploadResult;
            try {
                uploadResult = this.ossWriterProxy.initiateMultipartUpload(
                        uploadRequest);
            } catch (Exception e) {
                LOG.error("initiateMultipartUpload error: {}", e.getMessage(), e);
                throw DataXException.asDataXException(e.getMessage());
            }
            /**
             * 如果需要写同一个object，需要保证使用同一个upload Id
             * see: https://help.aliyun.com/document_detail/31993.html
             */
            String uploadId = uploadResult.getUploadId();
            OssSingleObject.uploadId = uploadId;
            LOG.info("writeSingleObject use uploadId: {}", uploadId);

            for (int i = 0; i < mandatoryNumber; i++) {
                Configuration splitedTaskConfig = this.writerSliceConfig
                        .clone();
                splitedTaskConfig.set(Key.OBJECT, object);
                splitedTaskConfig.set(Key.UPLOAD_ID, uploadId);
                writerSplitConfigs.add(splitedTaskConfig);
            }
            return writerSplitConfigs;
        }

        /**
         * osswriter多个task写多个object文件split逻辑，历史已有该逻辑，保持向前兼容性
         *
         * @param mandatoryNumber
         * @return
         */
        private List<Configuration> doSplitForWriteMultiObject(int mandatoryNumber) {
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            String bucket = this.writerSliceConfig.getString(Key.BUCKET);
            String object = this.writerSliceConfig.getString(Key.OBJECT);
            Set<String> allObjects = new HashSet<String>();
            try {
                List<OSSObjectSummary> ossObjectlisting = this.ossClient
                        .listObjects(bucket).getObjectSummaries();
                for (OSSObjectSummary objectSummary : ossObjectlisting) {
                    allObjects.add(objectSummary.getKey());
                }
            } catch (OSSException e) {
                throw DataXException.asDataXException(
                        OssWriterErrorCode.OSS_COMM_ERROR, e.getMessage(), e);
            } catch (ClientException e) {
                throw DataXException.asDataXException(
                        OssWriterErrorCode.OSS_COMM_ERROR, e.getMessage(), e);
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
            return writerSplitConfigs;
        }

        private boolean isPeer2PeerCopyMode() {
            return this.isBinaryFile
                    || com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.SYNC_MODE_VALUE_COPY
                    .equalsIgnoreCase(this.syncMode);
        }

        private String getObjectDir(String object) {
            String dir = null;
            if (StringUtils.isBlank(object)) {
                dir = "";
            } else {
                dir = object.trim();
                dir = dir.endsWith("/") ? dir : String.format("%s/", dir);
            }
            return dir;
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
        private String dateFormat;
        private DateFormat dateParse;
        private String fileFormat;
        private List<String> header;
        private Long maxFileSize;// MB
        private String suffix;
        private Boolean encrypt;// 是否在服务器端进行加密存储
        private long blockSizeInByte;
        private Boolean isBinaryFile;
        private String objectDir;
        private String syncMode;
        private int parentPathLength;
        private String byteEncoding;
        private HdfsWriter.Task hdfsWriterTask;
        private boolean useHdfsWriterProxy = false;
        private boolean writeSingleObject;
        private String uploadId;
        private OssWriterProxy ossWriterProxy;
        private List<String> partition;
        private boolean generateEmptyFile;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.fileFormat = this.writerSliceConfig
                    .getString(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_FORMAT,
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.FILE_FORMAT_TEXT);
            this.useHdfsWriterProxy  = HdfsParquetUtil.isUseHdfsWriterProxy(this.fileFormat);
            if(useHdfsWriterProxy){
                this.hdfsWriterTask = new HdfsWriter.Task();
                this.hdfsWriterTask.setPeerPluginJobConf(this.getPeerPluginJobConf());
                this.hdfsWriterTask.setPeerPluginName(this.getPeerPluginName());
                this.hdfsWriterTask.setPluginJobConf(this.getPluginJobConf());
                this.hdfsWriterTask.setReaderPluginSplitConf(this.getReaderPluginSplitConf());
                this.hdfsWriterTask.setTaskGroupId(this.getTaskGroupId());
                this.hdfsWriterTask.setTaskId(this.getTaskId());
                this.hdfsWriterTask.setTaskPluginCollector(this.getTaskPluginCollector());
                this.hdfsWriterTask.init();
                return;
            }

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
            this.encrypt = this.writerSliceConfig.getBool(Key.ENCRYPT, false);

            // 设置每块字符串长度
            this.blockSizeInByte = this.writerSliceConfig.getLong(Key.BLOCK_SIZE_IN_MB, 10L) * 1024 * 1024;

            this.isBinaryFile = this.writerSliceConfig.getBool(
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.BINARY, false);

            this.objectDir = this.getObjectDir(this.object);
            this.syncMode = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.SYNC_MODE, "");
            this.parentPathLength = this.writerSliceConfig.getInt("__parentPathLength", 0);

            this.byteEncoding = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.BYTE_ENCODING);

            this.writeSingleObject = this.writerSliceConfig.getBool(Key.WRITE_SINGLE_OBJECT, false);
            this.uploadId = this.writerSliceConfig.getString(Key.UPLOAD_ID);
            this.ossWriterProxy = new OssWriterProxy(this.writerSliceConfig, this.ossClient);
            this.partition = this.writerSliceConfig.getList(Key.PARTITION, new ArrayList<>(), String.class);
            //是否生成空文件开关
            this.generateEmptyFile = this.writerSliceConfig.getBool(Key.GENERATE_EMPTY_FILE,true);
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            if(useHdfsWriterProxy){
                hdfsWriterTask.startWrite(lineReceiver);
                return;
            }
            if (this.isPeer2PeerCopyMode()) {
                // 对等拷贝
                this.startWriteBinaryFile(lineReceiver);
            } else if (this.writeSingleObject) {
                this.startWriteSingleObjectUnstructedStorageFile(lineReceiver);
            } else {
                this.startWriteUnstructedStorageFile(lineReceiver,generateEmptyFile);
            }
        }

        /**
         * 单object写入
         *
         * @param lineReceiver
         */
        public void startWriteSingleObjectUnstructedStorageFile(RecordReceiver lineReceiver) {

            try {
                Record record;
                String currentObject = this.object;
                List<PartETag> currentPartETags = new ArrayList<PartETag>();

                //warn: may be StringBuffer->StringBuilder
                StringWriter sw = new StringWriter();
                StringBuffer sb = sw.getBuffer();
                UnstructuredWriter unstructuredWriter = UnstructuredStorageWriterUtil.
                        produceUnstructuredWriter(this.fileFormat, this.writerSliceConfig, sw);

                while ((record = lineReceiver.getFromReader()) != null) {
                    //单文件同步暂不支持轮转[目前单文件支持同步约最大100GB大小]
                    if (OssSingleObject.currentPartNumber.intValue() > Constant.MAX_BLOCK_SIZE) {
                        throw DataXException.asDataXException(String.format("When writeSingleObject is true, the write size of your single object has exceeded the maximum value of %s MB.",
                                (Constant.MAX_BLOCK_SIZE * this.blockSizeInByte / 1024 / 1024)));
                    }

                    // write: upload data to current object
                    UnstructuredStorageWriterUtil.transportOneRecord(record,
                            this.nullFormat, this.dateParse,
                            this.getTaskPluginCollector(), unstructuredWriter, this.byteEncoding);

                    // 达到 this.blockSizeInByte ，上传文件块
                    if (sb.length() >= this.blockSizeInByte) {
                        LOG.info(String
                                .format("write to bucket: [%s] object: [%s] with oss uploadId: [%s], currentPartNumber: %s",
                                        this.bucket, currentObject,
                                        this.uploadId, OssSingleObject.currentPartNumber.intValue()));
                        byte[] byteArray = sw.toString().getBytes(this.encoding);
                        this.ossWriterProxy.uploadOnePartForSingleObject(byteArray, this.uploadId, currentPartETags, currentObject, this::getHeaderBytes);
                        sb.setLength(0);
                    }
                }
                //将本task所有upload的part加入到allPartETags中
                OssSingleObject.allPartETags.addAll(currentPartETags);

                //将task未写完的最后一个block加入到 OssSingleObject.lastBlockBuffer 中，待job阶段合并上传
                if (sb.length() > 0) {
                    byte[] lastBlock = sw.toString().getBytes(this.encoding);
                    LOG.info("begin add last block to buffer, last block size: {}", lastBlock.length);
                    OssSingleObject.addLastBlockBuffer(lastBlock, this.ossWriterProxy, this.blockSizeInByte, this.object, this::getHeaderBytes);
                }
            } catch (IOException e) {
                // 脏数据UnstructuredStorageWriterUtil.transportOneRecord已经记录,header
                // 都是字符串不认为有脏数据
                throw DataXException.asDataXException(
                        OssWriterErrorCode.Write_OBJECT_ERROR, e.getMessage(), e);
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        OssWriterErrorCode.Write_OBJECT_ERROR, e.getMessage(), e);
            }
            LOG.info("single oss object end do write");
        }

        private byte[] getHeaderBytes() throws IOException {
            if (null != this.header && !this.header.isEmpty()) {
                // write header to writer
                try (StringWriter sw = new StringWriter();
                     UnstructuredWriter headerWriter = UnstructuredStorageWriterUtil.
                             produceUnstructuredWriter(this.fileFormat, this.writerSliceConfig, sw)) {
                    headerWriter.writeOneRecord(this.header);
                    return sw.toString().getBytes(this.encoding);
                }
            }
            return new byte[0];
        }

        /**
         * 同步音视频等无结构化文件
         * warn: 代码和startWriteUnstructedStorageFile重复程度太高，后续需要继续重构
         */
        private void startWriteBinaryFile(RecordReceiver lineReceiver) {
            Record record;
            String currentObject = null;
            InitiateMultipartUploadRequest currentInitiateMultipartUploadRequest;
            InitiateMultipartUploadResult currentInitiateMultipartUploadResult = null;
            String lastUploadId = null;
            boolean gotData = false;
            List<PartETag> currentPartETags = null;
            int currentPartNumber = 1;
            Map<String, String> meta;

            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            long currentSize = 0;
            try {
                // warn
                boolean needInitMultipartTransform = true;
                while ((record = lineReceiver.getFromReader()) != null) {
                    Column column = record.getColumn(0);
                    meta = record.getMeta();
                    assert meta != null;
                    gotData = true;
                    String objectNameTmp = meta
                            .get(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.META_KEY_FILE_PATH);
                    String fullObjectNameTmp = String.format("%s%s", this.objectDir, objectNameTmp.substring(this.parentPathLength, objectNameTmp.length()));

                    // init: 2 condition begin new multipart upload
                    if (needInitMultipartTransform || !StringUtils.equals(currentObject, fullObjectNameTmp)) {
                        // 先将上一个分块上传的request complete掉
                        if (null != currentInitiateMultipartUploadResult) {
                            // 如果还有部分分库数据没有提交，则先提交
                            if (currentSize > 0) {
                                this.ossWriterProxy.uploadOnePart(byteArrayOutputStream.toByteArray(), currentPartNumber,
                                        currentInitiateMultipartUploadResult, currentPartETags, currentObject);
                                currentPartNumber++;
                                currentSize = 0;
                                byteArrayOutputStream.reset();
                            }
                            // TODO 如果当前文件是空文件
                            String commitKey = currentInitiateMultipartUploadResult.getKey();
                            LOG.info(String.format(
                                    "current object [%s] size %s, complete current multipart upload %s and begin new one",
                                    commitKey, currentPartNumber * this.blockSizeInByte,
                                    currentInitiateMultipartUploadResult.getUploadId()));
                            CompleteMultipartUploadRequest currentCompleteMultipartUploadRequest = new CompleteMultipartUploadRequest(
                                    this.bucket, commitKey, currentInitiateMultipartUploadResult.getUploadId(),
                                    currentPartETags);
                            CompleteMultipartUploadResult currentCompleteMultipartUploadResult = this.ossWriterProxy.completeMultipartUpload(
                                    currentCompleteMultipartUploadRequest);
                            lastUploadId = currentInitiateMultipartUploadResult.getUploadId();
                            LOG.info(String.format("final object [%s] etag is:[%s]", commitKey,
                                    currentCompleteMultipartUploadResult.getETag()));
                        }
                        // 这里发现一个全新的文件需要分块上传
                        currentObject = fullObjectNameTmp;
                        currentInitiateMultipartUploadRequest = this.ossWriterProxy.getInitiateMultipartUploadRequest(currentObject);
                        currentInitiateMultipartUploadResult = this.ossWriterProxy.initiateMultipartUpload(
                                currentInitiateMultipartUploadRequest);
                        currentPartETags = new ArrayList<PartETag>();
                        LOG.info(String.format("write to bucket: [%s] object: [%s] with oss uploadId: [%s]",
                                this.bucket, currentObject, currentInitiateMultipartUploadResult.getUploadId()));
                        // warn
                        needInitMultipartTransform = false;
                        currentPartNumber = 1;
                    }
                    // write: upload data to current object
                    byte[] data;
                    if (column instanceof BytesColumn) {
                        data = column.asBytes();
                        byteArrayOutputStream.write(data);
                        currentSize += data.length;
                    } else {
                        String message = "the type of column must be BytesColumn!";
                        throw DataXException.asDataXException(OssWriterErrorCode.Write_OBJECT_ERROR, message);
                    }
                    if (currentSize >= this.blockSizeInByte) {
                        this.ossWriterProxy.uploadOnePart(byteArrayOutputStream.toByteArray(), currentPartNumber,
                                currentInitiateMultipartUploadResult, currentPartETags, currentObject);
                        currentPartNumber++;
                        currentSize = 0;
                        byteArrayOutputStream.reset();
                    }
                }

                // TODO binary 模式读取，源头为空文件时是有问题的
                if (!gotData) {
                    LOG.info("Receive no data from the source.");
                    currentInitiateMultipartUploadRequest = new InitiateMultipartUploadRequest(this.bucket,
                            currentObject);
                    currentInitiateMultipartUploadResult = this.ossWriterProxy.initiateMultipartUpload(
                            currentInitiateMultipartUploadRequest);
                    currentPartETags = new ArrayList<PartETag>();
                }

                // warn: may be some data stall in byteArrayOutputStream
                if (byteArrayOutputStream.size() > 0) {
                    this.ossWriterProxy.uploadOnePart(byteArrayOutputStream.toByteArray(), currentPartNumber,
                            currentInitiateMultipartUploadResult, currentPartETags, currentObject);
                    currentPartNumber++;
                }

                // 避免重复提交
                if (!StringUtils.equals(lastUploadId, currentInitiateMultipartUploadResult.getUploadId())) {
                    CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest(
                            this.bucket, currentObject, currentInitiateMultipartUploadResult.getUploadId(),
                            currentPartETags);
                    CompleteMultipartUploadResult completeMultipartUploadResult = this.ossWriterProxy.completeMultipartUpload(
                            completeMultipartUploadRequest);
                    LOG.info(String.format("final object etag is:[%s]", completeMultipartUploadResult.getETag()));
                }
            } catch (IOException e) {
                // 脏数据UnstructuredStorageWriterUtil.transportOneRecord已经记录,header
                // 都是字符串不认为有脏数据
                throw DataXException.asDataXException(OssWriterErrorCode.Write_OBJECT_ERROR, e.getMessage(), e);
            } catch (Exception e) {
                throw DataXException.asDataXException(OssWriterErrorCode.Write_OBJECT_ERROR, e.getMessage(), e);
            }
            LOG.info("end do write");
        }

        /**
         * 开始写半结构化文件
         *
         * @param lineReceiver
         */
        private void startWriteUnstructedStorageFile(RecordReceiver lineReceiver, boolean generateEmptyFile){
            // 设置每块字符串长度
            long numberCacul = (this.maxFileSize * 1024 * 1024L) / this.blockSizeInByte;
            final long maxPartNumber = numberCacul >= 1 ? numberCacul : 1;
            int objectRollingNumber = 0;
            Record record;
            String currentObject = this.object;
            if (this.isPeer2PeerCopyMode()) {
                currentObject = null;
            } else {
                // 加上suffix
                currentObject = appedSuffixTo(currentObject);
            }
            InitiateMultipartUploadRequest currentInitiateMultipartUploadRequest;
            InitiateMultipartUploadResult currentInitiateMultipartUploadResult = null;
            String lastUploadId = null;
            boolean gotData = false;
            List<PartETag> currentPartETags = null;
            // to do:
            // 可以根据currentPartNumber做分块级别的重试，InitiateMultipartUploadRequest多次一个currentPartNumber会覆盖原有
            int currentPartNumber = 1;
            Map<String, String> meta;

            //warn: may be StringBuffer->StringBuilder
            StringWriter sw = new StringWriter();
            StringBuffer sb = sw.getBuffer();
            UnstructuredWriter unstructuredWriter = UnstructuredStorageWriterUtil.
                    produceUnstructuredWriter(this.fileFormat, this.writerSliceConfig, sw);
            LOG.info(String.format(
                    "begin do write, each object maxFileSize: [%s]MB...",
                    maxPartNumber * 10));
            try {
                // warn 源头可能是MySQL中，导致没有meta这个第一次初始化标示省不掉
                boolean needInitMultipartTransform = true;
                while ((record = lineReceiver.getFromReader()) != null) {
                    meta = record.getMeta();
                    gotData = true;
                    // init: 2 condition begin new multipart upload 轮转策略(文件名规则)不一致
                    // condition: 对等拷贝模式 && Record中的Meta切换文件名 &&
                    // condition: 类log4j日志轮转 && !对等拷贝模式
                    boolean realyNeedInitUploadRequest = false;
                    if (this.isPeer2PeerCopyMode()) {
                        assert meta != null;
                        String objectNameTmp = meta
                                .get(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.META_KEY_FILE_PATH);
                        String fullObjectNameTmp = String.format("%s%s", this.objectDir, objectNameTmp.substring(this.parentPathLength, objectNameTmp.length()));
                        if (!StringUtils.equals(currentObject, fullObjectNameTmp)) {
                            currentObject = fullObjectNameTmp;
                            realyNeedInitUploadRequest = true;
                        }
                    } else {
                        if (needInitMultipartTransform || currentPartNumber > maxPartNumber) {
                            currentObject = getCurrentObject(objectRollingNumber, record);
                            objectRollingNumber++;
                            realyNeedInitUploadRequest = true;
                        }
                    }

                    if (realyNeedInitUploadRequest) {
                        // 先将上一个分块上传的request complete掉
                        if (null != currentInitiateMultipartUploadResult) {
                            if (sb.length() > 0) {
                                this.uploadOnePart(sw, currentPartNumber, currentInitiateMultipartUploadResult,
                                        currentPartETags, currentObject);
                                currentPartNumber++;
                                sb.setLength(0);
                            }
                            // TODO 如果当前文件是空文件
                            String commitKey = currentInitiateMultipartUploadResult.getKey();
                            LOG.info(String.format(
                                    "current object [%s] size %s, complete current multipart upload %s and begin new one",
                                    commitKey, currentPartNumber * this.blockSizeInByte,
                                    currentInitiateMultipartUploadResult.getUploadId()));
                            CompleteMultipartUploadRequest currentCompleteMultipartUploadRequest = new CompleteMultipartUploadRequest(
                                    this.bucket, commitKey, currentInitiateMultipartUploadResult.getUploadId(),
                                    currentPartETags);
                            CompleteMultipartUploadResult currentCompleteMultipartUploadResult = this.ossWriterProxy.completeMultipartUpload(
                                    currentCompleteMultipartUploadRequest);
                            lastUploadId = currentInitiateMultipartUploadResult.getUploadId();
                            LOG.info(String.format("final object [%s] etag is:[%s]", commitKey,
                                    currentCompleteMultipartUploadResult.getETag()));
                        }

                        currentInitiateMultipartUploadRequest = this.ossWriterProxy.getInitiateMultipartUploadRequest(currentObject);
                        currentInitiateMultipartUploadResult = this.ossWriterProxy.initiateMultipartUpload(currentInitiateMultipartUploadRequest);
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
                            this.getTaskPluginCollector(), unstructuredWriter, this.byteEncoding);

                    if (sb.length() >= this.blockSizeInByte) {
                        this.uploadOnePart(sw, currentPartNumber,
                                currentInitiateMultipartUploadResult,
                                currentPartETags, currentObject);
                        currentPartNumber++;
                        sb.setLength(0);
                    }
                }

                if (!gotData) {
                    LOG.info("Receive no data from the source.");
                    currentInitiateMultipartUploadRequest = new InitiateMultipartUploadRequest(
                            this.bucket, currentObject);
                    currentInitiateMultipartUploadResult = this.ossWriterProxy.initiateMultipartUpload(currentInitiateMultipartUploadRequest);
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

                // 避免重复提交
                if (!StringUtils.equals(lastUploadId, currentInitiateMultipartUploadResult.getUploadId())) {
                    CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest(
                            this.bucket, currentObject,
                            currentInitiateMultipartUploadResult.getUploadId(),
                            currentPartETags);
                    if (gotData) {
                        completeUpload(completeMultipartUploadRequest);
                    } else{
                        if (generateEmptyFile) {
                            LOG.info("Due to without data, oss will generate empty file, " +
                                    "the generateEmptyFile is {}, you can set it false to avoid this",generateEmptyFile);
                            completeUpload(completeMultipartUploadRequest);
                        } else {
                            LOG.info("The generateEmptyFile is false, datax will not generate empty file");
                        }
                    }
                }
            } catch (IOException e) {
                // 脏数据UnstructuredStorageWriterUtil.transportOneRecord已经记录,header
                // 都是字符串不认为有脏数据
                throw DataXException.asDataXException(
                        OssWriterErrorCode.Write_OBJECT_ERROR, e.getMessage(), e);
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        OssWriterErrorCode.Write_OBJECT_ERROR, e.getMessage(), e);
            }
            LOG.info("end do write");
        }

        private void completeUpload(CompleteMultipartUploadRequest completeMultipartUploadRequest) throws Exception {
            CompleteMultipartUploadResult completeMultipartUploadResult = this.ossWriterProxy.completeMultipartUpload(completeMultipartUploadRequest);
            LOG.info(String.format("final object etag is:[%s]",
                    completeMultipartUploadResult.getETag()));
        }


        private String getCurrentObject(int objectRollingNumber, Record record) {
            String currentObject  = this.object;

            if (!this.partition.isEmpty()) {
                String partitionValues = getPartitionValues(record);
                currentObject = String.format("%s_%s", currentObject, partitionValues);
            }

            if (objectRollingNumber > 0) {
                currentObject = String.format("%s_%s", currentObject, objectRollingNumber);
            }

            currentObject = appedSuffixTo(currentObject);

            return currentObject;
        }

        private String getPartitionValues(Record record) {
            // config like "partition": "ds,venture"
            String partitionValues = "";
            // assume that partition columns are located in the last of order
            for (int i = 0; i < this.partition.size(); i++) {
                partitionValues += record.getColumn(record.getColumnNumber() - 1 - i).asString();
            }
            return partitionValues;
        }

        private String appedSuffixTo(String currentObject) {
            StringBuilder sbCurrentObject = new StringBuilder(currentObject);

            if (StringUtils.isNotBlank(this.suffix)) {
                if (!this.suffix.startsWith(".")) {
                    sbCurrentObject.append(".");
                }
                sbCurrentObject.append(suffix);
            }

            return sbCurrentObject.toString();
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
                final InitiateMultipartUploadResult currentInitiateMultipartUploadResult,
                final List<PartETag> partETags, final String currentObject)
                throws Exception {
            final String encoding = this.encoding;
            final byte[] byteArray = sw.toString().getBytes(encoding);
            this.ossWriterProxy.uploadOnePart(byteArray, partNumber, currentInitiateMultipartUploadResult, partETags, currentObject);
        }

        @Override
        public void prepare() {
            if(useHdfsWriterProxy){
                hdfsWriterTask.prepare();
                return;
            }
        }

        @Override
        public void post() {
            if(useHdfsWriterProxy){
                hdfsWriterTask.post();
                return;
            }
        }

        @Override
        public void destroy() {
            if(useHdfsWriterProxy){
                hdfsWriterTask.destroy();
                return;
            }
            try {
                // this.ossClient.shutdown();
            } catch (Exception e) {
                LOG.warn("shutdown ossclient meet a exception:" + e.getMessage(), e);
            }
        }

        private boolean isPeer2PeerCopyMode() {
            return this.isBinaryFile
                    || com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.SYNC_MODE_VALUE_COPY
                    .equalsIgnoreCase(this.syncMode);
        }

        private String getObjectDir(String object) {
            String dir = null;
            if (StringUtils.isBlank(object)) {
                dir = "";
            } else {
                dir = object.trim();
                dir = dir.endsWith("/") ? dir : String.format("%s/", dir);
            }
            return dir;
        }
    }
}
