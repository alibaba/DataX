package com.alibaba.datax.plugin.writer;

public class DataHandlerFactory {

    public static DataHandler build(String peerPluginName) {
        if (peerPluginName.equals("opentsdbreader"))
            return new OpentsdbDataHandler();
        return new DefaultDataHandler();
    }
}
