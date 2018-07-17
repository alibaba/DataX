package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.transformer.ComplexTransformer;

/**
 * 单实例.
 * Created by liqiang on 16/3/9.
 */
public class TransformerInfo {

    /**
     * function基本信息
     */
    private ComplexTransformer transformer;
    private ClassLoader classLoader;
    private boolean isNative;


    public ComplexTransformer getTransformer() {
        return transformer;
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public boolean isNative() {
        return isNative;
    }

    public void setTransformer(ComplexTransformer transformer) {
        this.transformer = transformer;
    }

    public void setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    public void setIsNative(boolean isNative) {
        this.isNative = isNative;
    }
}
