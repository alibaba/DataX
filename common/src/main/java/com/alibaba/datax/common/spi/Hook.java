package com.alibaba.datax.common.spi;

import com.alibaba.datax.common.util.Configuration;

import java.util.Map;

/**
 * Created by xiafei.qiuxf on 14/12/17.
 * 钩子类
 */
public interface Hook {

    /**
     * 返回名字
     *
     * @return
     */
    String getName();

    /**
     * TODO 文档
     *
     * @param jobConf
     * @param msg
     */
    void invoke(Configuration jobConf, Map<String, Number> msg);

}
