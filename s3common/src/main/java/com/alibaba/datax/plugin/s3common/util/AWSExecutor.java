package com.alibaba.datax.plugin.s3common.util;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.text.StrPool;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.amazonaws.SDKGlobalConfiguration.ACCESS_KEY_ENV_VAR;
import static com.amazonaws.SDKGlobalConfiguration.SECRET_KEY_ENV_VAR;

/**
 * Author: duhanmin
 * Description: AWSExecutor
 * Date: 2021/5/19 11:53
 */
@Slf4j
public class AWSExecutor {

    private static final String AP_EAST_1 = "ap-east-1";
    private static final String FS_S3A_ACCESS_KEY = "fs.s3a.access.key";
    private static final String FS_S3A_SECRET_KEY = "fs.s3a.secret.key";
    private static final String FS_S3A_FAST_UPLOAD = "fs.s3a.fast.upload";
    private static final String PATH_SCHEMA = "s3a";

    private AmazonS3 s3Client;
    private Configuration conf = new Configuration();
    private String bucket;

    public AWSExecutor(String accessKey,String secretKey){
        if (StrUtil.isAllNotBlank(accessKey,secretKey)){
            AWSStaticCredentialsProvider credentials = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey,secretKey));
            this.s3Client = AmazonS3ClientBuilder.standard().withCredentials(credentials).withRegion(AP_EAST_1).build();
            conf.set(FS_S3A_ACCESS_KEY, accessKey);
            conf.set(FS_S3A_SECRET_KEY, secretKey);
            conf.set(FS_S3A_FAST_UPLOAD, Boolean.toString(true));
        }else {
            throw new SdkClientException("Unable to load AWS credentials from environment v ariables " + "(" + ACCESS_KEY_ENV_VAR + " and " + SECRET_KEY_ENV_VAR +  ")");
        }
    }

    public Configuration getConf() {
        return conf;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public Path path(String bucket, String prefix) {
        return new Path(PATH_SCHEMA, bucket, StrPool.SLASH + prefix);
    }

    public Path path(String prefix) {
        if (StrUtil.isBlank(this.bucket))
            throw new RuntimeException("bucket isBlank");
        return path(this.bucket, prefix);
    }

    public Path fileTypePath(String prefix,String fileType) {
        return path(prefix + StrPool.SLASH + getName(fileType));
    }

    /**
     * 写入文件命名
     * @return
     */
    public static String getName(String fileType){
        String objectSuffix = UUID.randomUUID().toString(true);
        String time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS"));
        StringBuilder sb = new StringBuilder();
        sb.append(time).append(StrPool.UNDERLINE).append(objectSuffix).append(StrPool.DOT).append(fileType);
        return sb.toString();
    }

    /**
     * s3查询文件列表
     *
     * @param bucket
     * @param prefix
     * @param maxKeys
     * @return
     */
    public List<String> list(@NonNull String bucket, String prefix, Integer maxKeys) {
        if (ObjectUtil.isEmpty(s3Client))  throw new RuntimeException("init s3Build()");
        ListObjectsV2Request req = new ListObjectsV2Request()
                .withBucketName(bucket)
                .withMaxKeys(maxKeys)
                .withPrefix(prefix);

        ListObjectsV2Result result = s3Client.listObjectsV2(req);

        try {
            log.info("s3 file list success");
            return result.getObjectSummaries().stream().map(S3ObjectSummary::getKey).collect(Collectors.toList());
        } catch (Throwable e) {
            log.error("get s3 file list failure", e);
            return Collections.emptyList();
        }
    }

    /**
     * s3查询文件列表
     *
     * @param bucket
     * @param prefix
     * @return
     */
    public List<String> list(@NonNull String bucket, String prefix) {
        return list(bucket,prefix,100);
    }


    /**
     * 删除指定目录清空文件夹
     *
     * @param bucket
     * @param prefix
     */
    public void delete(String bucket, String prefix) {
        List<S3ObjectSummary> objectSummaries = s3Client.listObjects(bucket, prefix).getObjectSummaries();
        for (S3ObjectSummary object : objectSummaries) {
            try {
                s3Client.deleteObject(bucket, object.getKey());
                log.info("s3 file delete success");
            } catch (Throwable e) {
                throw new RuntimeException("del error key:"+object.getKey(),e);
            }
        }
    }

    /**
     * 删除指定目录清空文件夹
     *
     * @param bucket
     * @param prefixs
     */
    public void delete(String bucket, List<String> prefixs) {
        for (String object : prefixs) {
            try {
                s3Client.deleteObject(bucket, object);
                log.info("s3 file delete success");
            } catch (Throwable e) {
                throw new RuntimeException("del error key:"+object,e);
            }
        }
    }

    /**
     * 获取文件
     *
     * @param key
     * @return
     */
    public S3ObjectInputStream getObject(String key){
        if (StrUtil.isBlank(this.bucket))
            throw new RuntimeException("bucket isBlank");
        return getObject(this.bucket,key);
    }

    /**
     * 获取文件
     *
     * @param bucketName
     * @param key
     * @return
     */
    public S3ObjectInputStream getObject(String bucketName, String key){
        return s3Client.getObject(bucketName,key).getObjectContent();
    }

    /**
     * s3写入文件
     *
     * @param bucketName
     * @param key
     * @param file
     */
    public void putFile(String bucketName, String key, File file) {
        if (FileUtil.exist(file))
            s3Client.putObject(bucketName, key + file.getName(), file);
        else if (FileUtil.size(file) == 0L){
            boolean bool = false;
            List<File> files = FileUtil.loopFiles(FileUtil.getParent(file, 1));
            for (File fileList:files){
                if (StrUtil.startWith(fileList.getName(),file.getName())) {
                    s3Client.putObject(bucketName, key + fileList.getName(), fileList);
                    bool = true;
                }
            }

            if (!bool){
                throw new RuntimeException("file isBlank"+file.getAbsolutePath());
            }
        } else throw new RuntimeException("file not exist"+file.getAbsolutePath());
    }

    /**
     * s3写入文件
     *
     * @param bucketName
     * @param key
     * @param content
     */
    public void putFile(String bucketName, String key, String content) {
        s3Client.putObject(bucketName, key, content);
    }

    /**
     *
     * @param bucket
     * @return
     */
    public boolean exist(String bucket) {
        try {
            s3Client.headBucket(new HeadBucketRequest(bucket));
            log.info("s3 file head success");
            return true;
        } catch (Throwable e) {
            e.printStackTrace();
            log.warn("S3 bucket not exist， exception：{}", e.getMessage());
            return false;
        }
    }

    public String acl(String bucket) {
        try {
            return s3Client.getBucketAcl(new GetBucketAclRequest(bucket)).getOwner().toString();
        } catch (Throwable e) {
            log.warn("S3 bucket ack exception ：{}", e.getMessage());
            return e.getMessage();
        }
    }
}
