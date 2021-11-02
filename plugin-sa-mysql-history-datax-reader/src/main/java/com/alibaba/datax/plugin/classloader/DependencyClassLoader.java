package com.alibaba.datax.plugin.classloader;

import com.alibaba.datax.BasePlugin;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import lombok.extern.slf4j.Slf4j;
import sun.net.www.ParseUtil;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public class DependencyClassLoader extends URLClassLoader {


    public DependencyClassLoader(URL[] urls, ClassLoader parent) {
        super(urls,parent);
    }

    public DependencyClassLoader(URL[] urls) {
        super(urls);
    }

    public static ClassLoader loadClassJar(String suffixPath){
        try {
            String curClassPath = DependencyClassLoader.class.getResource("").getFile();
            String rootPath = curClassPath.substring(curClassPath.lastIndexOf(":")+1, curClassPath.lastIndexOf("!"));
            String p = rootPath.substring(0, rootPath.lastIndexOf("/"));
            ClassLoader classLoader = DependencyClassLoader.getClassLoader(p+suffixPath);
            return classLoader;
        }catch (Exception e){
            e.printStackTrace();
            throw new DataXException(CommonErrorCode.CONFIG_ERROR,String.format("插件：%s配置有误，请检查！",suffixPath));
        }
    }

    public static BasePlugin.SAPlugin getBasePlugin(String path, String classFullName, Map<String,Object> param){
        try {
            String curClassPath = DependencyClassLoader.class.getResource("").getFile();
            String rootPath = curClassPath.substring(curClassPath.lastIndexOf(":")+1, curClassPath.lastIndexOf("!"));
            String p = rootPath.substring(0, rootPath.lastIndexOf("/"));
            log.info("path：{}/plugin/{}",p,path);
            ClassLoader pluginClassLoader = DependencyClassLoader.getClassLoader(p+"/plugin/"+path);
            Class<BasePlugin> clazz =  (Class<BasePlugin>)pluginClassLoader.loadClass(classFullName);
            BasePlugin basePlugin = clazz.newInstance();
            BasePlugin.SAPlugin instance = basePlugin.instance(param);
            return instance;
        }catch (Exception e){
            e.printStackTrace();
            throw new DataXException(CommonErrorCode.CONFIG_ERROR,String.format("插件：%s配置有误，请检查！",path));
        }
    }

    public static Object invoke(Class clazz,Object instance,String methodName,Class[] argsClazz,Object[] args){
        try {
            if(Objects.isNull(args) && args.length <=0){
                return clazz.getMethod(methodName).invoke(instance, args);
            }
            return clazz.getMethod(methodName,argsClazz).invoke(instance, args);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static ClassLoader getClassLoader(String classPath) {
        File file = new File(classPath);
        if (!file.exists() || !file.isDirectory()) {
            throw new RuntimeException("Please check if your path is correct! path:"+file.getAbsolutePath());
        }
        //获取所有的该目录下所有的jar文件
        File[] jars = Arrays.stream(file.listFiles())
                .filter((filePointer) -> filePointer.getName().endsWith(".jar"))
                .collect(Collectors.toList()).toArray(new File[0]);

        URL[] urls = new URL[jars.length];
        try {
            //将其所有的路径转换为URL
            for (int i = 0; i < urls.length; i++) {
                File f = null;
                f = jars[i].getCanonicalFile();
                urls[i] = ParseUtil.fileToEncodedURL(f);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new DependencyClassLoader(urls,Thread.currentThread().getContextClassLoader());
    }
}
