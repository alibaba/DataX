package com.alibaba.datax.core.util.container;


import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import org.apache.commons.lang3.StringUtils;

/**
 * PluginLoader简单工厂
 * @author fuyouj
 */
public class PluginLoaderFactory {

    private static final String JAR_LOADER = "jarLoader";
    private static final String PLUGIN_LOADER = "pluginLoader";

    public static ClassLoader create(Configuration pluginDescConf, PluginType pluginType, String pluginName) {

        check(pluginDescConf, pluginType, pluginName);

        String loadType = pluginDescConf.getString("loadType");

        if (JAR_LOADER.equalsIgnoreCase(loadType)) {
            String path = pluginDescConf.getString("path");
            return new JarLoader(new String[]{path});
        }

        if (PLUGIN_LOADER.equalsIgnoreCase(loadType)){
            return new PluginClassLoader();
        }
        throw DataXException.asDataXException(
                FrameworkErrorCode.RUNTIME_ERROR,
                String.format(
                        "%s插件[%s]无法加载，不支持的loadType,请检查程序代码",
                        pluginType, pluginName));
    }

    private static void check(Configuration pluginDescConf, PluginType pluginType, String pluginName) {

        String loadType = pluginDescConf.getString("loadType");
        if (StringUtils.isBlank(loadType)){
            throw DataXException.asDataXException(
                    FrameworkErrorCode.RUNTIME_ERROR,
                    String.format(
                            "%s插件[%s]无法加载，没有指定的loadType!",
                            pluginType, pluginName));
        }

        String pluginPath = pluginDescConf.getString("path");
        if (StringUtils.isBlank(pluginPath) && JAR_LOADER.equalsIgnoreCase(loadType)) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.RUNTIME_ERROR,
                    String.format(
                            "%s插件[%s]路径非法!",
                            pluginType, pluginName));
        }
    }
}
