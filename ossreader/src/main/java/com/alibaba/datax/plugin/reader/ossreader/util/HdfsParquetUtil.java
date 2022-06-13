package com.alibaba.datax.plugin.reader.ossreader.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.ossreader.Key;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * @Author: guxuan
 * @Date 2022-05-17 15:46
 */
public class HdfsParquetUtil {
    public static boolean isUseHdfsWriterProxy( String fileFormat){
        if("orc".equalsIgnoreCase(fileFormat) || "parquet".equalsIgnoreCase(fileFormat)){
            return true;
        }
        return false;
    }

    /**
     * 配置readerOriginConfig 适配hdfsreader读取oss parquet
     * https://help.aliyun.com/knowledge_detail/74344.html
     * @param readerOriginConfig
     */
    public static void adaptConfiguration(Configuration readerOriginConfig){
        String bucket = readerOriginConfig.getString(Key.BUCKET);
        String fs =String.format("oss://%s",bucket);
        readerOriginConfig.set(com.alibaba.datax.plugin.reader.hdfsreader.Key.DEFAULT_FS,fs);
        readerOriginConfig.set(com.alibaba.datax.plugin.reader.hdfsreader.Key.FILETYPE,
                readerOriginConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_FORMAT));
        /**
         *  "path"、 "column" 相互一致
         */
        JSONObject hadoopConfig = new JSONObject();
        hadoopConfig.put(Key.FS_OSS_ACCESSID,readerOriginConfig.getString(Key.ACCESSID));
        hadoopConfig.put(Key.FS_OSS_ACCESSKEY,readerOriginConfig.getString(Key.ACCESSKEY));
        hadoopConfig.put(Key.FS_OSS_ENDPOINT,readerOriginConfig.getString(Key.ENDPOINT));
        readerOriginConfig.set(Key.HDOOP_CONFIG,Configuration.from(JSON.toJSONString(hadoopConfig)));
    }
}
