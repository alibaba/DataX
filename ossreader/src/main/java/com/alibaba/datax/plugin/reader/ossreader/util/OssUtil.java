package com.alibaba.datax.plugin.reader.ossreader.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.ossreader.Constant;
import com.alibaba.datax.plugin.reader.ossreader.Key;
import com.alibaba.datax.plugin.reader.ossreader.OssReaderErrorCode;
import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.OSSClient;

/**
 * Created by mengxin.liumx on 2014/12/8.
 */
public class OssUtil {
    public static OSSClient initOssClient(Configuration conf) {
        String endpoint = conf.getString(Key.ENDPOINT);
        String accessId = conf.getString(Key.ACCESSID);
        String accessKey = conf.getString(Key.ACCESSKEY);
        ClientConfiguration ossConf = new ClientConfiguration();
        ossConf.setSocketTimeout(Constant.SOCKETTIMEOUT);
        
        // .aliyun.com, if you are .aliyun.ga you need config this
        String cname = conf.getString(Key.CNAME);
        if (StringUtils.isNotBlank(cname)) {
            List<String> cnameExcludeList = new ArrayList<String>();
            cnameExcludeList.add(cname);
            ossConf.setCnameExcludeList(cnameExcludeList);
        }

        OSSClient client = null;
        try {
            client = new OSSClient(endpoint, accessId, accessKey, ossConf);

        } catch (IllegalArgumentException e) {
            throw DataXException.asDataXException(
                    OssReaderErrorCode.ILLEGAL_VALUE, e.getMessage());
        }

        return client;
    }
}
