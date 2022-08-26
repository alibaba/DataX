package com.alibaba.datax.plugin.writer.osswriter;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * @Author: guxuan
 * @Date 2022-05-17 16:29
 */
public class OssWriterProxy {
    private static Logger logger = LoggerFactory.getLogger(OssWriterProxy.class);

    private OSSClient ossClient;
    private Configuration configuration;
    /**
     * 是否在服务器端进行加密存储
     */
    private Boolean encrypt;
    private String bucket;


    public OssWriterProxy (Configuration configuration, OSSClient ossClient) {
        this.configuration = configuration;
        this.ossClient = ossClient;
        this.encrypt = configuration.getBool(Key.ENCRYPT, false);
        this.bucket = configuration.getString(Key.BUCKET);
    }

    public InitiateMultipartUploadRequest getInitiateMultipartUploadRequest(String currentObject){
        InitiateMultipartUploadRequest currentInitiateMultipartUploadRequest;
        if( !this.encrypt ) {
            currentInitiateMultipartUploadRequest = new InitiateMultipartUploadRequest(
                    this.bucket, currentObject);
        } else {
            // 将数据加密存储在oss
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setHeader("x-oss-server-side-encryption",
                    ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
            currentInitiateMultipartUploadRequest = new InitiateMultipartUploadRequest(
                    this.bucket, currentObject, objectMetadata);
        }
        return currentInitiateMultipartUploadRequest;
    }

    public InitiateMultipartUploadResult initiateMultipartUpload(
            final InitiateMultipartUploadRequest currentInitiateMultipartUploadRequest) throws Exception {
        final OSSClient ossClient = this.ossClient;
        return RetryUtil.executeWithRetry(new Callable<InitiateMultipartUploadResult>() {
            @Override
            public InitiateMultipartUploadResult call() throws Exception {
                return ossClient.initiateMultipartUpload(currentInitiateMultipartUploadRequest);
            }
        }, 10, 1000L, false);
    }

    public CompleteMultipartUploadResult completeMultipartUpload(
            final CompleteMultipartUploadRequest currentCompleteMultipartUploadRequest) throws Exception {

        final OSSClient ossClient = this.ossClient;
        return RetryUtil.executeWithRetry(new Callable<CompleteMultipartUploadResult>() {
            @Override
            public CompleteMultipartUploadResult call() throws Exception {
                return ossClient.completeMultipartUpload(currentCompleteMultipartUploadRequest);
            }
        }, 10, 1000L, false);
    }

    public void uploadOnePart(
            final byte[] byteArray,
            final int partNumber,
            final InitiateMultipartUploadResult currentInitiateMultipartUploadResult,
            final List<PartETag> partETags,
            final String currentObject)
            throws Exception {
        final String bucket = this.bucket;
        final OSSClient ossClient = this.ossClient;
        RetryUtil.executeWithRetry(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                InputStream inputStream = new ByteArrayInputStream(
                        byteArray);
                // 创建UploadPartRequest，上传分块
                UploadPartRequest uploadPartRequest = new UploadPartRequest();
                uploadPartRequest.setBucketName(bucket);
                uploadPartRequest.setKey(currentObject);
                uploadPartRequest.setUploadId(currentInitiateMultipartUploadResult.getUploadId());
                uploadPartRequest.setInputStream(inputStream);
                uploadPartRequest.setPartSize(byteArray.length);
                uploadPartRequest.setPartNumber(partNumber);
                UploadPartResult uploadPartResult = ossClient
                        .uploadPart(uploadPartRequest);
                partETags.add(uploadPartResult.getPartETag());
                logger.info(String
                        .format("upload part [%s] size [%s] Byte has been completed.",
                                partNumber, byteArray.length));
                IOUtils.closeQuietly(inputStream);
                return true;
            }
        },  10, 1000L, false);
    }

    public void abortMultipartUpload(final String currentObject, final String uploadId) {
        final String bucket = this.bucket;
        final OSSClient ossClient = this.ossClient;
        try {
            RetryUtil.executeWithRetry((Callable<Void>) () -> {
                AbortMultipartUploadRequest abortMultipartUploadRequest =
                        new AbortMultipartUploadRequest(bucket, currentObject, uploadId);
                ossClient.abortMultipartUpload(abortMultipartUploadRequest);
                return null;
            }, 5, 1, true);
        } catch (Throwable e) {
            logger.error(String.format("AbortMultipartUpload failed, msg is %s",e.getMessage()), e);
        }
    }

    public void uploadOnePartForSingleObject(
            final byte[] byteArray,
            final String uploadId,
            final List<PartETag> partETags,
            final String currentObject,
            final HeaderProvider headerProvider)
            throws Exception {
        final String bucket = this.bucket;
        final OSSClient ossClient = this.ossClient;
        RetryUtil.executeWithRetry(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                // 创建UploadPartRequest，上传分块
                UploadPartRequest uploadPartRequest = new UploadPartRequest();
                uploadPartRequest.setPartNumber(OssSingleObject.currentPartNumber.getAndIncrement());
                byte[] data = byteArray;
                if (uploadPartRequest.getPartNumber() == 1) {
                    // write header
                    byte[] headerBytes = headerProvider.getHeader();
                    logger.info("write header to part {}. header size: {}",
                            uploadPartRequest.getPartNumber(), ArrayUtils.getLength(headerBytes));
                    data = ArrayUtils.addAll(headerBytes, byteArray);
                }
                ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
                uploadPartRequest.setBucketName(bucket);
                uploadPartRequest.setKey(currentObject);
                uploadPartRequest.setUploadId(uploadId);
                uploadPartRequest.setInputStream(inputStream);
                uploadPartRequest.setPartSize(data.length);
                UploadPartResult uploadPartResult = ossClient
                        .uploadPart(uploadPartRequest);
                partETags.add(uploadPartResult.getPartETag());
                logger.info("upload part number [{}] size [{}] Byte has been completed, uploadId: {}.",
                        uploadPartRequest.getPartNumber(), data.length, uploadId);
                IOUtils.closeQuietly(inputStream);
                return true;
            }
        },  10, 1000L, false);
    }

    public interface HeaderProvider {
        byte[] getHeader() throws Exception;
    }
}
