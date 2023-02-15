package com.alibaba.datax.plugin.reader.datahubreader;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.Account;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.http.HttpConfig;
import org.apache.commons.lang3.StringUtils;

public class DatahubClientHelper {
    public static DatahubClient getDatahubClient(Configuration jobConfig) {
        String accessId = jobConfig.getNecessaryValue(Key.CONFIG_KEY_ACCESS_ID,
                DatahubWriterErrorCode.MISSING_REQUIRED_VALUE);
        String accessKey = jobConfig.getNecessaryValue(Key.CONFIG_KEY_ACCESS_KEY,
                DatahubWriterErrorCode.MISSING_REQUIRED_VALUE);
        String endpoint = jobConfig.getNecessaryValue(Key.CONFIG_KEY_ENDPOINT,
                DatahubWriterErrorCode.MISSING_REQUIRED_VALUE);
        Account account = new AliyunAccount(accessId, accessKey);
        // 是否开启二进制传输，服务端2.12版本开始支持
        boolean enableBinary = jobConfig.getBool("enableBinary", false);
        DatahubConfig datahubConfig = new DatahubConfig(endpoint, account, enableBinary);
        // HttpConfig可不设置，不设置时采用默认值
        // 读写数据推荐打开网络传输 LZ4压缩
        HttpConfig httpConfig = null;
        String httpConfigStr = jobConfig.getString("httpConfig");
        if (StringUtils.isNotBlank(httpConfigStr)) {
            httpConfig = JSON.parseObject(httpConfigStr, new TypeReference<HttpConfig>() {
            });
        }

        DatahubClientBuilder builder = DatahubClientBuilder.newBuilder().setDatahubConfig(datahubConfig);
        if (null != httpConfig) {
            builder.setHttpConfig(httpConfig);
        }
        DatahubClient datahubClient = builder.build();
        return datahubClient;
    }
}
