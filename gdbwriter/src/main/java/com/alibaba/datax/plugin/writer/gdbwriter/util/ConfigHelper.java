/**
 * 
 */
package com.alibaba.datax.plugin.writer.gdbwriter.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.gdbwriter.GdbWriterErrorCode;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.apache.commons.lang3.StringUtils;

/**
 * @author jerrywang
 *
 */
public interface ConfigHelper {
	static void assertConfig(String key, Supplier<Boolean> f) {
		if (!f.get()) {
			throw DataXException.asDataXException(GdbWriterErrorCode.BAD_CONFIG_VALUE, key);
		}
	}

	static void assertHasContent(Configuration config, String key) {
		assertConfig(key, () -> StringUtils.isNotBlank(config.getString(key)));
	}

	/**
	 * NOTE: {@code Configuration::get(String, Class<T>)} doesn't work.
	 * 
	 * @param conf Configuration
	 * @param key key path to configuration
	 * @param cls Class of result type
	 * @return the target configuration object of type T
	 */
	static <T> T getConfig(Configuration conf, String key, Class<T> cls) {
		JSONObject j = (JSONObject) conf.get(key);
		return JSON.toJavaObject(j, cls);
	}
	
	/**
	 * Create a configuration from the specified file on the classpath.
	 * 
	 * @param name file name
	 * @return Configuration instance.
	 */
	static Configuration fromClasspath(String name) {
		try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(name)) {
			return Configuration.from(is);
		} catch (IOException e) {
			throw new IllegalArgumentException("File not found: " + name);
		}
	}
}
