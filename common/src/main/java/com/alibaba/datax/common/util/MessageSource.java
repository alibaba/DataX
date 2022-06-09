package com.alibaba.datax.common.util;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.TimeZone;

import org.apache.commons.lang3.LocaleUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MessageSource {
    private static final Logger LOG = LoggerFactory.getLogger(MessageSource.class);
    private static Map<String, ResourceBundle> resourceBundleCache = new HashMap<String, ResourceBundle>();
    public static Locale locale = null;
    public static TimeZone timeZone = null;
    private ResourceBundle resourceBundle = null;

    private MessageSource(ResourceBundle resourceBundle) {
        this.resourceBundle = resourceBundle;
    }

    /**
     * @param baseName
     *            demo: javax.servlet.http.LocalStrings
     * 
     * @throws MissingResourceException
     *             - if no resource bundle for the specified base name can be
     *             found
     * */
    public static MessageSource loadResourceBundle(String baseName) {
        return loadResourceBundle(baseName, MessageSource.locale,
                MessageSource.timeZone);
    }

    /**
     * @param clazz
     *            根据其获取package name
     * */
    public static <T> MessageSource loadResourceBundle(Class<T> clazz) {
        return loadResourceBundle(clazz.getPackage().getName());
    }

    /**
     * @param clazz
     *            根据其获取package name
     * */
    public static <T> MessageSource loadResourceBundle(Class<T> clazz,
            Locale locale, TimeZone timeZone) {
        return loadResourceBundle(clazz.getPackage().getName(), locale,
                timeZone);
    }

    /**
     * warn: 
     *   ok: ResourceBundle.getBundle("xxx.LocalStrings", Locale.getDefault(), LoadUtil.getJarLoader(PluginType.WRITER, "odpswriter"))
     *   error: ResourceBundle.getBundle("xxx.LocalStrings", Locale.getDefault(), LoadUtil.getJarLoader(PluginType.WRITER, "odpswriter"))
     * @param baseName
     *            demo: javax.servlet.http.LocalStrings
     * 
     * @throws MissingResourceException
     *             - if no resource bundle for the specified base name can be
     *             found
     *             
     * */
    public static MessageSource loadResourceBundle(String baseName,
            Locale locale, TimeZone timeZone) {
        ResourceBundle resourceBundle = null;
        if (null == locale) {
            locale = LocaleUtils.toLocale("en_US");
        }
        if (null == timeZone) {
            timeZone = TimeZone.getDefault();
        }
        String resourceBaseName = String.format("%s.LocalStrings", baseName);
        LOG.debug(
                "initEnvironment MessageSource.locale[{}], MessageSource.timeZone[{}]",
                MessageSource.locale, MessageSource.timeZone);
        LOG.debug(
                "loadResourceBundle with locale[{}], timeZone[{}], baseName[{}]",
                locale, timeZone, resourceBaseName);
        // warn: 这个map的维护需要考虑Local吗, no?
        if (!MessageSource.resourceBundleCache.containsKey(resourceBaseName)) {
            ClassLoader clazzLoader = Thread.currentThread()
                    .getContextClassLoader();
            LOG.debug("loadResourceBundle classLoader:{}", clazzLoader);
            resourceBundle = ResourceBundle.getBundle(resourceBaseName, locale,
                    clazzLoader);
            MessageSource.resourceBundleCache.put(resourceBaseName,
                    resourceBundle);
        } else {
            resourceBundle = MessageSource.resourceBundleCache
                    .get(resourceBaseName);
        }

        return new MessageSource(resourceBundle);
    }
    
    public static <T> boolean unloadResourceBundle(Class<T> clazz) {
        String baseName = clazz.getPackage().getName();
        String resourceBaseName = String.format("%s.LocalStrings", baseName);
        if (!MessageSource.resourceBundleCache.containsKey(resourceBaseName)) {
            return false;
        } else {
            MessageSource.resourceBundleCache.remove(resourceBaseName);
            return true;
        }
    }
    
    public static <T> MessageSource reloadResourceBundle(Class<T> clazz) {
        MessageSource.unloadResourceBundle(clazz);
        return MessageSource.loadResourceBundle(clazz);
    }

    public static void setEnvironment(Locale locale, TimeZone timeZone) {
        // warn: 设置默认?  @2018.03.21 将此处注释移除，否则在国际化多时区下会遇到问题
        Locale.setDefault(locale);
        TimeZone.setDefault(timeZone);
        MessageSource.locale = locale;
        MessageSource.timeZone = timeZone;
        LOG.info("use Locale: {} timeZone: {}", locale, timeZone);
    }

    public static void init(final Configuration configuration) {
        Locale locale2Set = Locale.getDefault();
        String localeStr = configuration.getString("common.column.locale", "zh_CN");// 默认操作系统的
        if (StringUtils.isNotBlank(localeStr)) {
            try {
                locale2Set = LocaleUtils.toLocale(localeStr);
            } catch (Exception e) {
                LOG.warn("ignored locale parse exception: {}", e.getMessage());
            }
        }

        TimeZone timeZone2Set = TimeZone.getDefault();
        String timeZoneStr = configuration.getString("common.column.timeZone");// 默认操作系统的
        if (StringUtils.isNotBlank(timeZoneStr)) {
            try {
                timeZone2Set = TimeZone.getTimeZone(timeZoneStr);
            } catch (Exception e) {
                LOG.warn("ignored timezone parse exception: {}", e.getMessage());
            }
        }

        LOG.info("JVM TimeZone: {}, Locale: {}", timeZone2Set.getID(), locale2Set);
        MessageSource.setEnvironment(locale2Set, timeZone2Set);
    }

    public static void clearCache() {
        MessageSource.resourceBundleCache.clear();
    }

    public String message(String code) {
        return this.messageWithDefaultMessage(code, null);
    }

    public String message(String code, String args1) {
        return this.messageWithDefaultMessage(code, null,
                new Object[] { args1 });
    }

    public String message(String code, String args1, String args2) {
        return this.messageWithDefaultMessage(code, null, new Object[] { args1,
                args2 });
    }

    public String message(String code, String args1, String args2, String args3) {
        return this.messageWithDefaultMessage(code, null, new Object[] { args1,
                args2, args3 });
    }

    // 上面几个重载可以应对大多数情况, 避免使用这个可以提高性能的
    public String message(String code, Object... args) {
        return this.messageWithDefaultMessage(code, null, args);
    }

    public String messageWithDefaultMessage(String code, String defaultMessage) {
        return this.messageWithDefaultMessage(code, defaultMessage,
                new Object[] {});
    }

    /**
     * @param args
     *            MessageFormat会依次调用对应对象的toString方法
     * */
    public String messageWithDefaultMessage(String code, String defaultMessage,
            Object... args) {
        String messageStr = null;
        try {
            messageStr = this.resourceBundle.getString(code);
        } catch (MissingResourceException e) {
            messageStr = defaultMessage;
        }
        if (null != messageStr && null != args && args.length > 0) {
            // warn: see loadResourceBundle set default locale
            return MessageFormat.format(messageStr, args);
        } else {
            return messageStr;
        }

    }
}
