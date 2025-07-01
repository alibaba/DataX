package com.alibaba.datax.example.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.transformer.ComplexTransformerProxy;
import com.alibaba.datax.core.transport.transformer.TransformerInfo;
import com.alibaba.datax.core.transport.transformer.TransformerRegistry;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.transformer.ComplexTransformer;
import com.alibaba.datax.transformer.Transformer;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.*;

/**
 * @author fuyouj
 */
public class ExampleConfigParser {
    private static final String CORE_CONF = "/example/conf/core.json";

    private static final String PLUGIN_DESC_FILE = "plugin.json";

    private static final String TRANSFORMER_DESC_FILE = "transformer.json";


    /**
     * 指定Job配置路径，ConfigParser会解析Job、Plugin、Core全部信息，并以Configuration返回
     * 不同于Core的ConfigParser,这里的core,plugin 不依赖于编译后的datax.home,而是扫描程序编译后的target目录
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
        // 扫描并注册自定义Transformer
        List<Configuration> listConfiguration = configuration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT_TRANSFORMER);
        registerTransformerConfig(listConfiguration);

        return configuration;
    }

    private static Configuration parsePluginsConfig(Map<String, String> pluginTypeMap) {

        Configuration configuration = Configuration.newDefault();

        //最初打算通过user.dir获取工作目录来扫描插件，
        //但是user.dir在不同有一些不确定性，所以废弃了这个选择

        for (File basePackage : runtimeBasePackages()) {
            if (pluginTypeMap.isEmpty()) {
                break;
            }
            scanPluginByPackage(basePackage, configuration, basePackage.listFiles(), pluginTypeMap);
        }
        if (!pluginTypeMap.isEmpty()) {
            String failedPlugin = pluginTypeMap.keySet().toString();
            String message = "\nplugin %s load failed ：ry to analyze the reasons from the following aspects.。\n" +
                    "1: Check if the name of the plugin is spelled correctly, and verify whether DataX supports this plugin\n" +
                    "2：Verify if the <resource></resource> tag has been added under <build></build> section in the pom file of the relevant plugin.\n<resource>" +
                    "                <directory>src/main/resources</directory>\n" +
                    "                <includes>\n" +
                    "                    <include>**/*.*</include>\n" +
                    "                </includes>\n" +
                    "                <filtering>true</filtering>\n" +
                    "            </resource>\n [Refer to the streamreader pom file] \n" +
                    "3: Check that the datax-yourPlugin-example module imported your test plugin";
            message = String.format(message, failedPlugin);
            throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_INIT_ERROR, message);
        }
        return configuration;
    }

    /**
     * 通过classLoader获取程序编译的输出目录
     *
     * @return File[/datax-example/target/classes,xxReader/target/classes,xxWriter/target/classes]
     */
    private static File[] runtimeBasePackages() {
        List<File> basePackages = new ArrayList<>();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Enumeration<URL> resources = null;
        try {
            resources = classLoader.getResources("");
        } catch (IOException e) {
            throw DataXException.asDataXException(e.getMessage());
        }

        while (resources.hasMoreElements()) {
            URL resource = resources.nextElement();
            File file = new File(resource.getFile());
            if (file.isDirectory()) {
                basePackages.add(file);
            }
        }

        return basePackages.toArray(new File[0]);
    }

    /**
     * @param packageFile       编译出来的target/classes根目录 便于找到插件时设置插件的URL目录，设置根目录是最保险的方式
     * @param configuration     pluginConfig
     * @param files             待扫描文件
     * @param needPluginTypeMap 需要的插件
     */
    private static void scanPluginByPackage(File packageFile,
                                            Configuration configuration,
                                            File[] files,
                                            Map<String, String> needPluginTypeMap) {
        if (files == null) {
            return;
        }
        for (File file : files) {
            if (file.isFile() && PLUGIN_DESC_FILE.equals(file.getName())) {
                Configuration pluginDesc = Configuration.from(file);
                String descPluginName = pluginDesc.getString("name", "");

                if (needPluginTypeMap.containsKey(descPluginName)) {

                    String type = needPluginTypeMap.get(descPluginName);
                    configuration.merge(parseOnePlugin(packageFile.getAbsolutePath(), type, descPluginName, pluginDesc), false);
                    needPluginTypeMap.remove(descPluginName);

                }
            } else {
                scanPluginByPackage(packageFile, configuration, file.listFiles(), needPluginTypeMap);
            }
        }
    }


    private static Configuration parseOnePlugin(String packagePath,
                                                String pluginType,
                                                String pluginName,
                                                Configuration pluginDesc) {
        //设置path 兼容jarLoader的加载方式URLClassLoader
        pluginDesc.set("path", packagePath);
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

    /**
     * 注册第三方 transformer
     * 判断是否使用TransFormer
     * 如果使用则将涉及的Transformer项目全部注册
     *
     * @param transformers
     */
    private static void registerTransformerConfig(List<Configuration> transformers) {

        if (transformers == null || transformers.size() == 0) {
            return;
        }
        Set<String> transformerSet = new HashSet<>();
        for (Configuration transformer : transformers) {
            String name = transformer.getString("name");
            if (!name.startsWith("dx_")) { // 只检测自定义Transformer
                transformerSet.add(name);
            }
        }
        for (File basePackage : runtimeBasePackages()) {
            scanTransFormerByPackage(basePackage, basePackage.listFiles(), transformerSet);
        }
        if (!transformerSet.isEmpty()) {
            String failedTransformer = transformerSet.toString();
            String message = "\ntransformer %s load failed ：ry to analyze the reasons from the following aspects.。\n" +
                    "1: Check if the name of the transformer is spelled correctly, and verify whether DataX supports this transformer\n" +
                    "2：Verify if the <resource></resource> tag has been added under <build></build> section in the pom file of the relevant transformer.\n<resource>" +
                    "                <directory>src/main/resources</directory>\n" +
                    "                <includes>\n" +
                    "                    <include>**/*.*</include>\n" +
                    "                </includes>\n" +
                    "                <filtering>true</filtering>\n" +
                    "            </resource>\n [Refer to the streamreader pom file] \n" +
                    "3: Check that the datax-yourtransformer-example module imported your test transformer";
            message = String.format(message, failedTransformer);
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_INIT_ERROR, message);
        }
    }

    /**
     * @param packageFile 编译出来的target/classes根目录 便于找到TRANSFORMER时设置插件的TRANSFORMER 目录，设置根目录是最保险的方式
     * @param files       待扫描文件
     */
    private static void scanTransFormerByPackage(File packageFile, File[] files, Set<String> transformerSet) {
        if (files == null) {
            return;
        }
        for (File file : files) {
            if (file.isFile() && TRANSFORMER_DESC_FILE.equals(file.getName())) {
                Configuration transfomerDesc = Configuration.from(file);
                String descTransformerName = transfomerDesc.getString("name");
                String descTransformerClass = transfomerDesc.getString("class");
                transformerSet.remove(descTransformerName);

                if (verfiyExist(descTransformerName,descTransformerClass)) {
                    break;
                }
                try {
                    Class<?> transformerClass = Class.forName(descTransformerClass);
                    Object transformer = transformerClass.newInstance();
                    if (ComplexTransformer.class.isAssignableFrom(transformer.getClass())) {
                        ((ComplexTransformer) transformer).setTransformerName(descTransformerName);
                        TransformerRegistry.registComplexTransformer((ComplexTransformer) transformer, null, false);
                    } else if (Transformer.class.isAssignableFrom(transformer.getClass())) {
                        ((Transformer) transformer).setTransformerName(descTransformerName);
                        TransformerRegistry.registTransformer((Transformer) transformer, null, false);
                    } else {
                        throw DataXException.asDataXException(String.format("load Transformer class(%s) error, path = %s", descTransformerClass, file.getPath()));
                    }
                } catch (Exception e) {
                    //错误funciton跳过
                    throw DataXException.asDataXException(String.format("skip transformer(%s),load Transformer class error, path = %s ", descTransformerName, file.getPath()));
                }
            } else {
                scanTransFormerByPackage(packageFile, file.listFiles(), transformerSet);
            }
        }
    }

    /**
     * 验证Transformer 是否已经加载过了
     * @param transformerName
     * @param transFormerClass
     * @return
     */
    private static Boolean verfiyExist(String transformerName,String transFormerClass){
        TransformerInfo transformer = TransformerRegistry.getTransformer(transformerName);
        if(transformer==null){
            return false;
        }
        ComplexTransformerProxy proxy = (ComplexTransformerProxy)transformer.getTransformer();
        if(proxy==null){
            return false;
        }
        String className = proxy.getRealTransformer().getClass().getName();
        if (transFormerClass.equals(className)){
            return true;
        }else{
            throw DataXException.asDataXException(String.format("skip transformer(%s),load Transformer class error,There are two Transformer with the same name and different class path, class1 = %s;class2 = %s ", transformerName,transFormerClass,className));
        }
    }

}
