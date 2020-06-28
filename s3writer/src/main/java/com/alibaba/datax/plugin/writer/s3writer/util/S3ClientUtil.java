package com.alibaba.datax.plugin.writer.s3writer.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.s3writer.Key;
import com.alibaba.datax.plugin.writer.s3writer.S3Writer;
import com.alibaba.datax.plugin.writer.s3writer.S3WriterErrorCode;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * S3 Client Util
 *
 * @author L.cm
 */
public class S3ClientUtil {
    private static final Logger log = LoggerFactory.getLogger(S3Writer.Job.class);

    public static S3Client initOssClient(Configuration conf) {
        String accessKey = conf.getString(Key.ACCESSKEY);
        String secretKey = conf.getString(Key.SECRETKEY);
        String region = conf.getString(Key.REGION);
        AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(accessKey, secretKey);
        return S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(awsCredentials)).region(Region.of(region))
                .build();
    }

    public static boolean doesBucketExist(S3Client s3Client, String bucket) {
        try {
            s3Client.headBucket(builder -> builder.bucket(bucket));
            log.info("s3 file head success");
            return true;
        } catch (Throwable e) {
            e.printStackTrace();
            log.warn("S3 bucket 不存在， 异常信息为：{}", e.getMessage());
            return false;
        }
    }

    public static String getBucketAcl(S3Client s3Client, String bucket) {
        try {
            GetBucketAclResponse bucketAcl = s3Client.getBucketAcl(builder -> builder.bucket(bucket));
            return bucketAcl.owner().toString();
        } catch (Throwable e) {
            log.warn("S3 bucket ack 异常， 异常信息为：{}", e.getMessage());
            return e.getMessage();
        }
    }

    public static List<String> listObjects(S3Client s3Client, String bucket) {
        return listObjects(s3Client, bucket, null);
    }

    public static List<String> listObjects(S3Client s3Client, String bucket, String prefix) {
        // 一次只拉取100个
        ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder()
                .maxKeys(100)
                .bucket(bucket);
        if (StringUtils.isNotBlank(prefix)) {
            builder.prefix(prefix);
        }
        ListObjectsV2Request objectsV2Request = builder.build();
        try {
            ListObjectsV2Response listObjects = s3Client.listObjectsV2(objectsV2Request);
            log.info("s3 file list success");
            return listObjects.contents().stream().map(S3Object::key).collect(Collectors.toList());
        } catch (Throwable e) {
            log.error("S3文件列表拉取失败", e);
            return Collections.emptyList();
        }
    }

    public static void deleteObjects(S3Client s3Client, String bucket, List<String> keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        List<ObjectIdentifier> objectIdentifierList = keys.stream()
                .map(key -> ObjectIdentifier.builder().key(key).build())
                .collect(Collectors.toList());
        Delete delete = Delete.builder().objects(objectIdentifierList).build();
        try {
            s3Client.deleteObjects(builder -> builder.bucket(bucket).delete(delete));
            log.info("s3 file delete success");
        } catch (Throwable e) {
            throw DataXException.asDataXException(S3WriterErrorCode.OSS_COMM_ERROR, e.getMessage());
        }
    }

    public static void uploadObject(S3Client s3Client, String bucket, String key, InputStream in, Long size) {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(bucket).key(key).build();
        RequestBody requestBody = RequestBody.fromInputStream(in, size);
        try {
            s3Client.putObject(putObjectRequest, requestBody);
            log.info("s3 file upload success");
        } catch (Throwable e) {
            throw DataXException.asDataXException(S3WriterErrorCode.OSS_COMM_ERROR, e.getMessage());
        }
    }

    public static UploadPartResponse uploadPart(S3Client s3Client, UploadPartRequest uploadPartRequest, RequestBody requestBody) {
        try {
            UploadPartResponse uploadPartResponse = s3Client.uploadPart(uploadPartRequest, requestBody);
            log.info("s3 file upload part success");
            return uploadPartResponse;
        } catch (Throwable e) {
            throw DataXException.asDataXException(S3WriterErrorCode.OSS_COMM_ERROR, e.getMessage());
        }
    }

    public static String createMultipartUpload(S3Client s3Client, String bucket, String currentObject) {
        CreateMultipartUploadRequest uploadRequest = CreateMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(currentObject)
                .build();
        try {
            CreateMultipartUploadResponse multipartUpload = s3Client.createMultipartUpload(uploadRequest);
            log.info("s3 file create upload multi part success");
            return multipartUpload.uploadId();
        } catch (Throwable e) {
            throw DataXException.asDataXException(S3WriterErrorCode.Write_OBJECT_ERROR, e.getMessage());
        }
    }

    public static CompleteMultipartUploadResponse completeMultipartUpload(S3Client s3Client, String bucket, String currentObject,
                                                                          String uploadId, List<CompletedPart> eTagList) {
        CompletedMultipartUpload multipartUpload = CompletedMultipartUpload.builder()
                .parts(eTagList).build();
        CompleteMultipartUploadRequest request = CompleteMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(currentObject)
                .uploadId(uploadId)
                .multipartUpload(multipartUpload)
                .build();
        try {
            CompleteMultipartUploadResponse response = s3Client.completeMultipartUpload(request);
            log.info("s3 file complete upload multi part success");
            return response;
        } catch (Throwable e) {
            throw DataXException.asDataXException(S3WriterErrorCode.Write_OBJECT_ERROR, e.getMessage());
        }
    }
}
