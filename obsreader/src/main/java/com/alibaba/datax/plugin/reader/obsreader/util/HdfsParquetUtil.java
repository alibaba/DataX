package com.alibaba.datax.plugin.reader.obsreader.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.obsreader.Key;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class HdfsParquetUtil {
    public static boolean isUseHdfsWriterProxy( String fileFormat){
        if("orc".equalsIgnoreCase(fileFormat) || "parquet".equalsIgnoreCase(fileFormat)){
            return true;
        }
        return false;
    }

    /**
     * 配置readerOriginConfig 适配hdfsreader读取obs parquet
     * https://help.aliyun.com/knowledge_detail/74344.html
     * @param readerOriginConfig
     */
    public static void adaptConfiguration(Configuration readerOriginConfig){
        String bucket = readerOriginConfig.getString(Key.BUCKET);
        String fs =String.format("obs://%s",bucket);
        readerOriginConfig.set(com.alibaba.datax.plugin.reader.hdfsreader.Key.DEFAULT_FS,fs);
        readerOriginConfig.set(com.alibaba.datax.plugin.reader.hdfsreader.Key.FILETYPE,
                readerOriginConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_FORMAT));
        /**
         *  "path"、 "column" 相互一致
         */
        JSONObject hadoopConfig = new JSONObject();
        hadoopConfig.put(Key.FS_OBS_ACCESSKEY,readerOriginConfig.getString(Key.ACCESSKEY));
        hadoopConfig.put(Key.FS_OBS_SECRETKEY,readerOriginConfig.getString(Key.SECRETKEY));
        hadoopConfig.put(Key.FS_OBS_ENDPOINT,readerOriginConfig.getString(Key.ENDPOINT));
        readerOriginConfig.set(Key.HDOOP_CONFIG,Configuration.from(JSON.toJSONString(hadoopConfig)));
    }
}
