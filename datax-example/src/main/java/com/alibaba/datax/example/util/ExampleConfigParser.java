package com.alibaba.datax.example.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;

import java.io.File;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * @author fuyouj
 */
public class ExampleConfigParser {
    private static final String CORE_CONF = "/example/conf/core.json";

    private static final String PLUGIN_DESC_FILE = "plugin.json";

    /**
     * 指定Job配置路径，ConfigParser会解析Job、Plugin、Core全部信息，并以Configuration返回
     * 不同于Core的ConfigParser,这里的core,plugin 不依赖于编译后的datax.home,而是扫描程序目录
     */
    public static Configuration parse(final String jobPath) {

        Configuration configuration = ConfigParser.parseJobConfig(jobPath);
        configuration.merge(coreConfig(),
                false);

        Map<String, String> pluginTypeMap = new HashMap<>();
        String readerName = configuration.getString(CoreConstant.DATAX_JOB_CONTENT_READER_NAME);
        String writerName = configuration.getString(CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME);
        pluginTypeMap.put(readerName, "reader");
        pluginTypeMap.put(writerName, "writer");
        Configuration pluginsDescConfig = parsePluginsConfig(pluginTypeMap);
        configuration.merge(pluginsDescConfig, false);
        return configuration;
    }

    private static Configuration parsePluginsConfig(Map<String, String> pluginTypeMap) {

        Configuration configuration = Configuration.newDefault();

        String workingDirectory = System.getProperty("user.dir");
        File file = new File(workingDirectory);
        scanPlugin(configuration, file.listFiles(), pluginTypeMap);
        if (!pluginTypeMap.isEmpty()) {
            throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_INIT_ERROR,
                    "load plugin failed，未完成指定插件加载:"
                            + pluginTypeMap.keySet());
        }
        return configuration;
    }

    private static void scanPlugin(Configuration configuration, File[] files, Map<String, String> pluginTypeMap) {
        if (files == null) {
            return;
        }
        for (File file : files) {
            if (file.isFile() && PLUGIN_DESC_FILE.equals(file.getName())) {
                Configuration pluginDesc = Configuration.from(file);
                String descPluginName = pluginDesc.getString("name", "");

                if (pluginTypeMap.containsKey(descPluginName)) {

                    String type = pluginTypeMap.get(descPluginName);
                    configuration.merge(parseOnePlugin(type, descPluginName, pluginDesc), false);
                    pluginTypeMap.remove(descPluginName);

                }
            } else {
                scanPlugin(configuration, file.listFiles(), pluginTypeMap);
            }
        }
    }

    private static Configuration parseOnePlugin(String pluginType, String pluginName, Configuration pluginDesc) {

        pluginDesc.set("loadType","pluginLoader");
        Configuration pluginConfInJob = Configuration.newDefault();
        pluginConfInJob.set(
                String.format("plugin.%s.%s", pluginType, pluginName),
                pluginDesc.getInternal());
        return pluginConfInJob;
    }

    private static Configuration coreConfig() {
        try {
            URL resource = ExampleConfigParser.class.getResource(CORE_CONF);
            return Configuration.from(Paths.get(resource.toURI()).toFile());
        } catch (Exception ignore) {
            throw DataXException.asDataXException("Failed to load the configuration file core.json. " +
                    "Please check whether /example/conf/core.json exists!");
        }
    }
}
