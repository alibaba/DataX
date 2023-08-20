package com.alibaba.datax.example;


import com.alibaba.datax.example.util.PathUtil;

/**
 * @author fuyouj
 */
public class Main {

    /**
     * 1.在example模块pom文件添加你依赖的的调试插件，
     * 你可以直接打开本模块的pom文件,参考是如何引入streamreader，streamwriter
     * 2. 在此处指定你的job文件
     */
    public static void main(String[] args) {

        String classPathJobPath = "/job/stream2stream.json";
        String absJobPath = PathUtil.getAbsolutePathFromClassPath(classPathJobPath);
        ExampleContainer.start(absJobPath);
    }

}
