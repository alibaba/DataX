package com.alibaba.datax.plugin.reader.obsreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.obsreader.Constant;
import com.alibaba.datax.plugin.reader.obsreader.Key;
import com.alibaba.datax.plugin.reader.obsreader.ObsReaderErrorCode;
import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class ObsUtil {

    /**
     * 初始化obs
     * @param conf
     * @return
     */
    public static ObsClient initObsClient(Configuration conf) {
        String endpoint = conf.getString(Key.ENDPOINT);
        String accessKey = conf.getString(Key.ACCESSKEY);
        String secretKey = conf.getString(Key.SECRETKEY);

        ObsConfiguration obsConf = new ObsConfiguration();
        obsConf.setEndPoint(endpoint);
        obsConf.setSocketTimeout(Constant.SOCKETTIMEOUT);
        obsConf.setMaxErrorRetry(1);

        ObsClient client = null;
        try {
//            client = new ObsClient(accessKey, secretKey, endpoint);
            client = new ObsClient(accessKey, secretKey, obsConf);
        } catch (IllegalArgumentException e) {
            throw DataXException.asDataXException(
                    ObsReaderErrorCode.ILLEGAL_VALUE, e.getMessage());
        }

        return client;
    }
}
