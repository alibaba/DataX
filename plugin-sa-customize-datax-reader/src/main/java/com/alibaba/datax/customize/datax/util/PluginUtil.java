package com.alibaba.datax.customize.datax.util;

import com.alibaba.datax.reader.common.CommonReader;

import java.io.Serializable;

public class PluginUtil implements Serializable {

    private static final long serialVersionUID = -2501670961541188453L;

    private PluginUtil(){}

    private static CommonReader.SAReaderPlugin PLUGIN = null;

    public static void setPlugin(CommonReader.SAReaderPlugin plugin){
        PLUGIN = plugin;
    }

    public static CommonReader.SAReaderPlugin plugin(){
        return PLUGIN;
    }
}
