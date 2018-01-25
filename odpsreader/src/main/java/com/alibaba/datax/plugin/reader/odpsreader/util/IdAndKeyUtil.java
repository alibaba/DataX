/**
 *  (C) 2010-2014 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.datax.plugin.reader.odpsreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.odpsreader.Constant;
import com.alibaba.datax.plugin.reader.odpsreader.Key;
import com.alibaba.datax.plugin.reader.odpsreader.OdpsReaderErrorCode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class IdAndKeyUtil {
    private static Logger LOG = LoggerFactory.getLogger(IdAndKeyUtil.class);

    public static Configuration parseAccessIdAndKey(Configuration originalConfig) {
        String accessId = originalConfig.getString(Key.ACCESS_ID);
        String accessKey = originalConfig.getString(Key.ACCESS_KEY);

        // 只要 accessId,accessKey 二者配置了一个，就理解为是用户本意是要直接手动配置其 accessid/accessKey
        if (StringUtils.isNotBlank(accessId) || StringUtils.isNotBlank(accessKey)) {
            LOG.info("Try to get accessId/accessKey from your config.");
            //通过如下语句，进行检查是否确实配置了
            accessId = originalConfig.getNecessaryValue(Key.ACCESS_ID, OdpsReaderErrorCode.REQUIRED_VALUE);
            accessKey = originalConfig.getNecessaryValue(Key.ACCESS_KEY, OdpsReaderErrorCode.REQUIRED_VALUE);
            //检查完毕，返回即可
            return originalConfig;
        } else {
            Map<String, String> envProp = System.getenv();
            return getAccessIdAndKeyFromEnv(originalConfig, envProp);
        }
    }

    private static Configuration getAccessIdAndKeyFromEnv(Configuration originalConfig,
                                                          Map<String, String> envProp) {
        String accessId = null;
        String accessKey = null;

        String skynetAccessID = envProp.get(Constant.SKYNET_ACCESSID);
        String skynetAccessKey = envProp.get(Constant.SKYNET_ACCESSKEY);

        if (StringUtils.isNotBlank(skynetAccessID)
                || StringUtils.isNotBlank(skynetAccessKey)) {
            /**
             * 环境变量中，如果存在SKYNET_ACCESSID/SKYNET_ACCESSKEy（只要有其中一个变量，则认为一定是两个都存在的！），
             * 则使用其值作为odps的accessId/accessKey(会解密)
             */

            LOG.info("Try to get accessId/accessKey from environment.");
            accessId = skynetAccessID;
            accessKey = DESCipher.decrypt(skynetAccessKey);
            if (StringUtils.isNotBlank(accessKey)) {
                originalConfig.set(Key.ACCESS_ID, accessId);
                originalConfig.set(Key.ACCESS_KEY, accessKey);
                LOG.info("Get accessId/accessKey from environment variables successfully.");
            } else {
                throw DataXException.asDataXException(OdpsReaderErrorCode.GET_ID_KEY_FAIL,
                        String.format("从环境变量中获取accessId/accessKey 失败, accessId=[%s]", accessId));
            }
        } else {
            // 无处获取（既没有配置在作业中，也没用在环境变量中）
            throw DataXException.asDataXException(OdpsReaderErrorCode.GET_ID_KEY_FAIL,
                    "无法获取到accessId/accessKey. 它们既不存在于您的配置中，也不存在于环境变量中.");
        }

        return originalConfig;
    }
}
