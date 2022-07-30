package com.alibaba.datax.common.util;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;

public class IdAndKeyRollingUtil {
	private static Logger LOGGER = LoggerFactory.getLogger(IdAndKeyRollingUtil.class);
	public static final String SKYNET_ACCESSID = "SKYNET_ACCESSID";
	public static final String SKYNET_ACCESSKEY = "SKYNET_ACCESSKEY";

	public final static String ACCESS_ID = "accessId";
	public final static String ACCESS_KEY = "accessKey";

	public static String parseAkFromSkynetAccessKey() {
		Map<String, String> envProp = System.getenv();
		String skynetAccessID = envProp.get(IdAndKeyRollingUtil.SKYNET_ACCESSID);
		String skynetAccessKey = envProp.get(IdAndKeyRollingUtil.SKYNET_ACCESSKEY);
		String accessKey = null;
		// follow 原有的判断条件
		// 环境变量中，如果存在SKYNET_ACCESSID/SKYNET_ACCESSKEy（只要有其中一个变量，则认为一定是两个都存在的！
		// if (StringUtils.isNotBlank(skynetAccessID) ||
		// StringUtils.isNotBlank(skynetAccessKey)) {
		// 检查严格，只有加密串不为空的时候才进去，不过 之前能跑的加密串都不应该为空
		if (StringUtils.isNotBlank(skynetAccessKey)) {
			LOGGER.info("Try to get accessId/accessKey from environment SKYNET_ACCESSKEY.");
			accessKey = DESCipher.decrypt(skynetAccessKey);
			if (StringUtils.isBlank(accessKey)) {
				// 环境变量里面有，但是解析不到
				throw DataXException.asDataXException(String.format(
						"Failed to get the [accessId]/[accessKey] from the environment variable. The [accessId]=[%s]",
						skynetAccessID));
			}
		}
		if (StringUtils.isNotBlank(accessKey)) {
			LOGGER.info("Get accessId/accessKey from environment variables SKYNET_ACCESSKEY successfully.");
		}
		return accessKey;
	}

	public static String getAccessIdAndKeyFromEnv(Configuration originalConfig) {
		String accessId = null;
		Map<String, String> envProp = System.getenv();
		accessId = envProp.get(IdAndKeyRollingUtil.SKYNET_ACCESSID);
		String accessKey = null;
		if (StringUtils.isBlank(accessKey)) {
			// 老的没有出异常，只是获取不到ak
			accessKey = IdAndKeyRollingUtil.parseAkFromSkynetAccessKey();
		}

		if (StringUtils.isNotBlank(accessKey)) {
			// 确认使用这个的都是 accessId、accessKey的命名习惯
			originalConfig.set(IdAndKeyRollingUtil.ACCESS_ID, accessId);
			originalConfig.set(IdAndKeyRollingUtil.ACCESS_KEY, accessKey);
		}
		return accessKey;
	}
}
