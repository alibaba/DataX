package com.alibaba.datax.common.util;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.TimeZone;
import org.apache.commons.lang3.LocaleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageSource {
	private static final Logger LOG = LoggerFactory.getLogger(MessageSource.class);
	private static Map<String, ResourceBundle> resourceBundleCache = new HashMap<String, ResourceBundle>();
	private static Locale locale = null;
	private static TimeZone timeZone = null;
	private ResourceBundle resourceBundle = null;

	private MessageSource(ResourceBundle resourceBundle) {
		this.resourceBundle = resourceBundle;
	}

	public static MessageSource loadResourceBundle(String baseName) {
		return loadResourceBundle(baseName, locale, timeZone);
	}

	public static <T> MessageSource loadResourceBundle(Class<T> clazz) {
		return loadResourceBundle(clazz.getPackage().getName());
	}

	public static <T> MessageSource loadResourceBundle(Class<T> clazz, Locale locale, TimeZone timeZone) {
		return loadResourceBundle(clazz.getPackage().getName(), locale, timeZone);
	}

	public static MessageSource loadResourceBundle(String baseName, Locale locale, TimeZone timeZone) {
		ResourceBundle resourceBundle = null;
		if (null == locale) {
			locale = Locale.getDefault();
		}
		if (null == timeZone) {
			timeZone = TimeZone.getDefault();
		}
		String resourceBaseName = String.format("%s.LocalStrings", new Object[] { baseName });
		LOG.debug("initEnvironment MessageSource.locale[{}], MessageSource.timeZone[{}]", locale, timeZone);

		LOG.debug("loadResourceBundle with locale[{}], timeZone[{}], baseName[{}]",
				new Object[] { locale, timeZone, resourceBaseName });
		if (!resourceBundleCache.containsKey(resourceBaseName)) {
			ClassLoader clazzLoader = Thread.currentThread().getContextClassLoader();

			LOG.debug("loadResourceBundle classLoader:{}", clazzLoader);
			resourceBundle = ResourceBundle.getBundle(resourceBaseName, locale, clazzLoader);

			resourceBundleCache.put(resourceBaseName, resourceBundle);
		} else {
			resourceBundle = (ResourceBundle) resourceBundleCache.get(resourceBaseName);
		}
		return new MessageSource(resourceBundle);
	}

	public static void setEnvironment(Locale locale, TimeZone timeZone) {
		MessageSource.locale = locale;
		MessageSource.timeZone = timeZone;
	}

	public static void init(Configuration configuration) {
		String localeStr = configuration.getString("common.column.locale", "zh_CN");

		String timeZoneStr = configuration.getString("common.column.timeZone", "GMT+8");

		setEnvironment(LocaleUtils.toLocale(localeStr), TimeZone.getTimeZone(timeZoneStr));
	}

	public static void clearCache() {
		resourceBundleCache.clear();
	}

	public String message(String code) {
		return messageWithDefaultMessage(code, null);
	}

	public String message(String code, String args1) {
		return messageWithDefaultMessage(code, null, new Object[] { args1 });
	}

	public String message(String code, String args1, String args2) {
		return messageWithDefaultMessage(code, null, new Object[] { args1, args2 });
	}

	public String message(String code, String args1, String args2, String args3) {
		return messageWithDefaultMessage(code, null, new Object[] { args1, args2, args3 });
	}

	public String message(String code, Object... args) {
		return messageWithDefaultMessage(code, null, args);
	}

	public String messageWithDefaultMessage(String code, String defaultMessage) {
		return messageWithDefaultMessage(code, defaultMessage, new Object[0]);
	}

	public String messageWithDefaultMessage(String code, String defaultMessage, Object... args) {
		String messageStr = null;
		try {
			messageStr = this.resourceBundle.getString(code);
		} catch (MissingResourceException e) {
			messageStr = defaultMessage;
		}
		if ((null != messageStr) && (null != args) && (args.length > 0)) {
			return MessageFormat.format(messageStr, args);
		}
		return messageStr;
	}
}