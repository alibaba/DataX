/**
 *  (C) 2010-2022 Alibaba Group Holding Limited.
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

package com.alibaba.datax.plugin.writer.odpswriter.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.IdAndKeyRollingUtil;
import com.alibaba.datax.common.util.MessageSource;
import com.alibaba.datax.plugin.writer.odpswriter.Key;
import com.alibaba.datax.plugin.writer.odpswriter.OdpsWriterErrorCode;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class IdAndKeyUtil {
    private static Logger LOG = LoggerFactory.getLogger(IdAndKeyUtil.class);
    private static final MessageSource MESSAGE_SOURCE = MessageSource.loadResourceBundle(IdAndKeyUtil.class);

    public static Configuration parseAccessIdAndKey(Configuration originalConfig) {
        String accessId = originalConfig.getString(Key.ACCESS_ID);
        String accessKey = originalConfig.getString(Key.ACCESS_KEY);

        // 只要 accessId,accessKey 二者配置了一个，就理解为是用户本意是要直接手动配置其 accessid/accessKey
        if (StringUtils.isNotBlank(accessId) || StringUtils.isNotBlank(accessKey)) {
            LOG.info("Try to get accessId/accessKey from your config.");
            //通过如下语句，进行检查是否确实配置了
            accessId = originalConfig.getNecessaryValue(Key.ACCESS_ID, OdpsWriterErrorCode.REQUIRED_VALUE);
            accessKey = originalConfig.getNecessaryValue(Key.ACCESS_KEY, OdpsWriterErrorCode.REQUIRED_VALUE);
            //检查完毕，返回即可
            return originalConfig;
        } else {
            Map<String, String> envProp = System.getenv();
            return getAccessIdAndKeyFromEnv(originalConfig, envProp);
        }
    }

    private static Configuration getAccessIdAndKeyFromEnv(Configuration originalConfig,
                                                          Map<String, String> envProp) {
    	// 如果获取到ak，在getAccessIdAndKeyFromEnv中已经设置到originalConfig了
    	String accessKey = IdAndKeyRollingUtil.getAccessIdAndKeyFromEnv(originalConfig);
    	if (StringUtils.isBlank(accessKey)) {
    		// 无处获取（既没有配置在作业中，也没用在环境变量中）
            throw DataXException.asDataXException(OdpsWriterErrorCode.GET_ID_KEY_FAIL,
                    MESSAGE_SOURCE.message("idandkeyutil.2"));
    	}
        return originalConfig;
    }
}
