package com.alibaba.datax.plugin.classloader;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import sun.net.www.ParseUtil;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

public class HiveDependencyClassLoader extends URLClassLoader {


    public HiveDependencyClassLoader(URL[] urls, ClassLoader parent) {
        super(urls,parent);
    }

    public HiveDependencyClassLoader(URL[] urls) {
        super(urls);
    }

    public static ClassLoader loadClassJar(String suffixPath){
        try {
            String curClassPath = HiveDependencyClassLoader.class.getResource("").getFile();
            String rootPath = curClassPath.substring(curClassPath.lastIndexOf(":")+1, curClassPath.lastIndexOf("!"));
            String p = rootPath.substring(0, rootPath.lastIndexOf("/"));
            ClassLoader classLoader = HiveDependencyClassLoader.getClassLoader(p+"/mysqllib/"+suffixPath);
            return classLoader;
        }catch (Exception e){
            e.printStackTrace();
            throw new DataXException(CommonErrorCode.CONFIG_ERROR,String.format("插件：%s配置有误，请检查！",suffixPath));
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
        return new HiveDependencyClassLoader(urls,Thread.currentThread().getContextClassLoader());
    }
}
