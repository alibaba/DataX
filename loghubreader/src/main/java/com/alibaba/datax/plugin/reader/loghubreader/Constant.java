package com.alibaba.datax.plugin.reader.loghubreader;

public class Constant {

    public static String DATETIME_FORMAT = "yyyyMMddHHmmss";
    public static String DATE_FORMAT = "yyyyMMdd";

    static String META_COL_SOURCE = "__source__";
    static String META_COL_TOPIC = "__topic__";
    static String META_COL_CATEGORY = "__category__";
    static String META_COL_MACHINEUUID = "__machineUUID__";
    static String META_COL_HOSTNAME = "__hostname__";
    static String META_COL_PATH = "__path__";
    static String META_COL_LOGTIME = "__logtime__";
    public static String META_COL_RECEIVE_TIME = "__receive_time__";

    /**
     * 除用户手动配置的列之外，其余数据列作为一个 json 读取到一列
     */
    static String COL_EXTRACT_OTHERS = "C__extract_others__";

    /**
     * 将所有元数据列作为一个 json 读取到一列
     */
    static String COL_EXTRACT_ALL_META = "C__extract_all_meta__";
}
