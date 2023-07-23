package com.alibaba.datax.core.util.container;


/**
 * @author fuyouj
 */
public interface PluginLoader {
    /**
     * 加载插件对象
     *
     * @param name 类全限定名
     * @return class对象
     * @throws ClassNotFoundException
     */
    Class<?> loadClass(String name) throws ClassNotFoundException;
}
