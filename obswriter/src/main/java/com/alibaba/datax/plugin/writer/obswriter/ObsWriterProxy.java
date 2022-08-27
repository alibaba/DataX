package com.alibaba.datax.plugin.writer.obswriter;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.obs.services.ObsClient;
import com.obs.services.model.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.Callable;

public class ObsWriterProxy {
    private static Logger logger = LoggerFactory.getLogger(ObsWriterProxy.class);

    private ObsClient obsClient;
    private Configuration configuration;
    /**
     * 是否在服务器端进行加密存储
     */
    private Boolean encrypt;
    private String bucket;


    public ObsWriterProxy(Configuration configuration, ObsClient obsClient) {
        this.configuration = configuration;
        this.obsClient = obsClient;
        this.encrypt = configuration.getBool(Key.ENCRYPT, false);
        this.bucket = configuration.getString(Key.BUCKET);
    }

    public InitiateMultipartUploadRequest getInitiateMultipartUploadRequest(String currentObject){
        InitiateMultipartUploadRequest currentInitiateMultipartUploadRequest;

        if( !this.encrypt ) {
            currentInitiateMultipartUploadRequest = new InitiateMultipartUploadRequest(
                    this.bucket, currentObject);
        } else {
            // 将数据加密存储在obs
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.addUserMetadata("x-obs-server-side-encryption", "kms");
            currentInitiateMultipartUploadRequest = new InitiateMultipartUploadRequest(
                    this.bucket, currentObject);
            currentInitiateMultipartUploadRequest.setMetadata(objectMetadata);
        }

        return currentInitiateMultipartUploadRequest;
    }

    public InitiateMultipartUploadResult initiateMultipartUpload(
            final InitiateMultipartUploadRequest currentInitiateMultipartUploadRequest) throws Exception {
        final ObsClient obsClient = this.obsClient;
        return RetryUtil.executeWithRetry(new Callable<InitiateMultipartUploadResult>() {
            @Override
            public InitiateMultipartUploadResult call() throws Exception {
                return obsClient.initiateMultipartUpload(currentInitiateMultipartUploadRequest);
            }
        }, 10, 1000L, false);
    }

    public CompleteMultipartUploadResult completeMultipartUpload(
            final CompleteMultipartUploadRequest currentCompleteMultipartUploadRequest) throws Exception {

        final ObsClient obsClient = this.obsClient;
        return RetryUtil.executeWithRetry(new Callable<CompleteMultipartUploadResult>() {
            @Override
            public CompleteMultipartUploadResult call() throws Exception {
                return obsClient.completeMultipartUpload(currentCompleteMultipartUploadRequest);
            }
        }, 10, 1000L, false);
    }

    public void uploadOnePart(
            final byte[] byteArray,
            final int partNumber,
            final InitiateMultipartUploadResult currentInitiateMultipartUploadResult,
            final List<PartEtag> partETags,
            final String currentObject)
            throws Exception {
        final String bucket = this.bucket;
        final ObsClient obsClient = this.obsClient;
        RetryUtil.executeWithRetry(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                InputStream inputStream = new ByteArrayInputStream(
                        byteArray);
                // 创建UploadPartRequest，上传分块
                UploadPartRequest uploadPartRequest = new UploadPartRequest();
                uploadPartRequest.setBucketName(bucket);
                uploadPartRequest.setObjectKey(currentObject);
                uploadPartRequest.setUploadId(currentInitiateMultipartUploadResult.getUploadId());
                uploadPartRequest.setInput(inputStream);
                uploadPartRequest.setPartSize((long)byteArray.length);
                uploadPartRequest.setPartNumber(partNumber);
                UploadPartResult uploadPartResult = obsClient
                        .uploadPart(uploadPartRequest);
                partETags.add(new PartEtag(uploadPartResult.getEtag(), uploadPartResult.getPartNumber()));
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
        final ObsClient obsClient = this.obsClient;
        try {
            RetryUtil.executeWithRetry((Callable<Void>) () -> {
                AbortMultipartUploadRequest abortMultipartUploadRequest =
                        new AbortMultipartUploadRequest(bucket, currentObject, uploadId);
                obsClient.abortMultipartUpload(abortMultipartUploadRequest);
                return null;
            }, 5, 1, true);
        } catch (Throwable e) {
            logger.error(String.format("AbortMultipartUpload failed, msg is %s",e.getMessage()), e);
        }
    }

    public void uploadOnePartForSingleObject(
            final byte[] byteArray,
            final String uploadId,
            final List<PartEtag> partETags,
            final String currentObject,
            final HeaderProvider headerProvider)
            throws Exception {
        final String bucket = this.bucket;
        final ObsClient obsClient = this.obsClient;
        RetryUtil.executeWithRetry(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                // 创建UploadPartRequest，上传分块
                UploadPartRequest uploadPartRequest = new UploadPartRequest();
                uploadPartRequest.setPartNumber(ObsSingleObject.currentPartNumber.getAndIncrement());
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
                uploadPartRequest.setObjectKey(currentObject);
                uploadPartRequest.setUploadId(uploadId);
                uploadPartRequest.setInput(inputStream);
                uploadPartRequest.setPartSize((long)data.length);
                UploadPartResult uploadPartResult = obsClient
                        .uploadPart(uploadPartRequest);
                partETags.add(new PartEtag(uploadPartResult.getEtag(), uploadPartResult.getPartNumber()));
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
