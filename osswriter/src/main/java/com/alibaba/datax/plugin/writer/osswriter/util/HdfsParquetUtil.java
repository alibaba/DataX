package com.alibaba.datax.plugin.writer.osswriter.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsWriter;
import com.alibaba.datax.plugin.writer.osswriter.Key;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: guxuan
 * @Date 2022-05-17 16:35
 */
public class HdfsParquetUtil {


    private static final Logger logger = LoggerFactory.getLogger(HdfsParquetUtil.class);

    public static boolean isUseHdfsWriterProxy( String fileFormat){
        if("orc".equalsIgnoreCase(fileFormat) || "parquet".equalsIgnoreCase(fileFormat)){
            return true;
        }
        return false;
    }

    /**
     * 配置writerSliceConfig 适配hdfswriter写oss parquet
     * https://help.aliyun.com/knowledge_detail/74344.html
     * @param hdfsWriterJob
     * @param writerSliceConfig
     */
    public static void adaptConfiguration(HdfsWriter.Job hdfsWriterJob, Configuration writerSliceConfig){
        String fileFormat = writerSliceConfig.getString(
                com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_FORMAT,
                com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.FILE_FORMAT_TEXT);

        String bucket = writerSliceConfig.getString(Key.BUCKET);
        String fs =String.format("oss://%s",bucket);
        writerSliceConfig.set(com.alibaba.datax.plugin.writer.hdfswriter.Key.DEFAULT_FS,fs);
        writerSliceConfig.set(com.alibaba.datax.plugin.writer.hdfswriter.Key.FILE_TYPE,
                writerSliceConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_FORMAT));

        /**
         *  "writeMode"、 "compress"、"encoding" 、path、fileName 相互一致
         */
        JSONObject hadoopConfig = new JSONObject();
        hadoopConfig.put(Key.FS_OSS_ACCESSID,writerSliceConfig.getString(Key.ACCESSID));
        hadoopConfig.put(Key.FS_OSS_ACCESSKEY,writerSliceConfig.getString(Key.ACCESSKEY));
        hadoopConfig.put(Key.FS_OSS_ENDPOINT,writerSliceConfig.getString(Key.ENDPOINT));
        writerSliceConfig.set(Key.HDOOP_CONFIG,Configuration.from(JSON.toJSONString(hadoopConfig)));

        String object = writerSliceConfig.getString(Key.OBJECT);
        String path = writerSliceConfig.getString(Key.PATH);
        String fielName = writerSliceConfig.getString(Key.FILE_NAME);

        if (StringUtils.isNotBlank(object) && (StringUtils.isNotBlank(path) || StringUtils.isNotBlank(fielName))) {
            logger.warn("You configure both the \"object\" property and the \"path\" or \"fileName\" property, ignoring the object property. "
                    + "It is recommended to remove the \"path\" or \"fileName\" attribute, which has been deprecated.");
        }

        //兼容之前配置了PATH的datax任务, 如果已经配置了PATH，则无需从object里解析
        if (StringUtils.isBlank(path)) {
            Validate.notBlank(object, "object can't be blank!");
            writerSliceConfig.set(Key.PATH, getPathAndFileNameFromObject(object.trim()).get(Key.PATH));
        }
        //兼容之前配置了fileName的datax任务，如果已经配置了fileName，则无需从object里解析
        if (StringUtils.isBlank(fielName)) {
            Validate.notBlank(object, "object can't be blank!");
            writerSliceConfig.set(Key.FILE_NAME, getPathAndFileNameFromObject(object.trim()).get(Key.FILE_NAME));
        }
        if (StringUtils.equalsIgnoreCase(fileFormat, "parquet")) {
            hdfsWriterJob.unitizeParquetConfig(writerSliceConfig);
        }

    }



    /**
     * 从object中 解析出 path和fileName
     *
     * 举例1：
     * /hello/aaa/bbb/ccc.txt
     * path: /hello/aaa/bbb
     * fileName: ccc.txt
     *
     * 举例2:
     * hello/aaa/bbb/ccc.txt
     * path: /hello/aaa/bbb
     * fileName: ccc.txt
     *
     * 举例3:
     * ccc.txt
     * path: /
     * fileName: ccc.txt
     *
     * 举例4:
     * /ccc.txt
     * path: /
     * fileName: ccc.txt
     *
     * @param object
     * @return
     */
    public static Map<String, String> getPathAndFileNameFromObject(String object) {
        Map<String, String> pathAndFileName = new HashMap<>();

        boolean isContainsBackslash = object.contains("/");

        //object里没有包含"/", 则将path设置为 "/", fileName设置为 object
        if (!isContainsBackslash) {
            pathAndFileName.put(Key.PATH, "/");
            pathAndFileName.put(Key.FILE_NAME, object);
            return pathAndFileName;
        }

        if (!object.startsWith("/")) {
            object = "/" + object;
        }

        int lastIndex = object.lastIndexOf("/");
        String path = object.substring(0, lastIndex);
        String fileName = object.substring(lastIndex + 1);

        path = StringUtils.isNotBlank(path) ? path : "/";

        logger.info("path: {}", path);
        logger.info("fileName: {}", fileName);

        pathAndFileName.put(Key.PATH, path);
        pathAndFileName.put(Key.FILE_NAME, fileName);
        return pathAndFileName;
    }
}
