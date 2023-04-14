package com.alibaba.datax.plugin.rdbms.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Key;

import java.util.Map;
import java.util.Properties;

/**
 *
 * @desc
 * @date 3/9/2023 10:30  
 * @author Blade
 */
public final class ConfigUtil {
	public static Properties getJdbcProperties(Configuration originalConfig) {
		Properties prop = new Properties();
		Map<String, Object> jdbcConfigMap = originalConfig.getMap(Key.JDBC_Config);
		if(jdbcConfigMap!=null && !jdbcConfigMap.isEmpty()){
			prop.putAll(jdbcConfigMap);
		}
		return prop;
	}
}
