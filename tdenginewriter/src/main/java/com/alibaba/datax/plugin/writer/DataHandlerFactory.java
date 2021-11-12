package com.alibaba.datax.plugin.writer;

public class DataHandlerFactory {

    public static DataHandler build(String peerPluginName) {
        if (peerPluginName.equals("opentsdbreader"))
            return new OpentsdbDataHandler();
        if (peerPluginName.equals("mysqlreader"))
            return new MysqlDataHandler();
        return new DefaultDataHandler();
    }
}
