package com.alibaba.datax.plugin.writer.hologresjdbcwriter.util;

import com.alibaba.hologres.client.model.WriteMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

public class ConfLoader {
	public static Logger LOG = LoggerFactory.getLogger(ConfLoader.class);

	static public <T> T load(Map<String, Object> props, T config, Set<String> ignoreList) throws Exception {
		Field[] fields = config.getClass().getDeclaredFields();
		for (Map.Entry<String, Object> entry : props.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue().toString();
			if (ignoreList.contains(key)) {
				LOG.info("Config Skip {}", key);
				continue;
			}
			boolean match = false;
			for (Field field : fields) {
				if (field.getName().equals(key)) {
					match = true;
					field.setAccessible(true);
					Class<?> type = field.getType();
					if (type.equals(String.class)) {
						field.set(config, value);
					} else if (type.equals(int.class)) {
						field.set(config, Integer.parseInt(value));
					} else if (type.equals(long.class)) {
						field.set(config, Long.parseLong(value));
					} else if (type.equals(boolean.class)) {
						field.set(config, Boolean.parseBoolean(value));
					} else if (WriteMode.class.equals(type)) {
						field.set(config, WriteMode.valueOf(value));
					} else {
						throw new Exception("invalid type " + type + " for param " + key);
					}
					if ("password".equals(key)) {
						StringBuilder sb = new StringBuilder();
						for (int i = 0; i < value.length(); ++i) {
							sb.append("*");
						}
						LOG.info("Config {}={}", key, sb.toString());
					} else {
						LOG.info("Config {}={}", key, value);
					}
				}
			}
			if (!match) {
				throw new Exception("param " + key + " not found in HoloConfig");
			}
		}
		return config;
	}
}
