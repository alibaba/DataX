package com.alibaba.datax.plugin;

public class KeyConstant {

    /**
     * 神策数据存放路径
     */
    public static final String SDK_DATA_ADDRESS = "sdkDataAddress";
    /**
     * hive表名
     */
    public static final String TABLE = "table";
    /**
     * 查询的列
     */
    public static final String COLUMN = "column";
    /**
     * 导入神策的列
     */
    public static final String SA_COLUMN = "column";
    /**
     * 同步的类型（track/user/item）
     */
    public static final String URL = "url";
    /**
     * event类型
     */
    public static final String TRACK = "track";
    /**
     * item类型
     */
    public static final String ITEM = "item";
    /**
     *  distinctId
     */
    public static final String DISTINCT_ID_COLUMN = "distinctIdColumn";
    /**
     * distinctId列是否是登录后的id
     */
    public static final String IS_LOGIN_ID = "isLoginId";
    /**
     * event类型的事件名
     */
    public static final String EVENT_NAME = "eventName";
    /**
     * user类型
     */
    public static final String USER = "user";
    /**
     * item类型的itemType字段列，可以是常量，对应的TYPE_IS_COLUMN属性为false
     */
    public static final String ITEM_TYPE = "itemType";
    /**
     * item类型的itemId字段列
     */
    public static final String ITEM_ID_COLUMN = "itemIdColumn";
    /**
     * item类型的itemType字段是否是查询列，否则是常量
     */
    public static final String TYPE_IS_COLUMN = "typeIsColumn";
    /**
     * 小数点
     */
    public static final String POINT = ".";
    /**
     * 内部event类型使用
     */
    public static final String EVENT_DISTINCT_ID_COL = "eventDistinctIdCol";
    /**
     *内部event类型使用
     */
    public static final String EVENT_IS_LOGIN_ID = "eventIsLoginId";
    /**
     *内部event类型使用
     */
    public static final String EVENT_EVENT_NAME = "eventEventName";

    /**
     * 内部user类型使用
     */
    public static final String USER_DISTINCT_ID = "userDistinctId";
    /**
     * 内部user类型使用
     */
    public static final String user_is_login_id = "userIsLoginId";
    /**
     * 内部item类型使用
     */
    public static final String ITEM_ITEM_TYPE = "itemItemType";
    /**
     * 内部item类型使用
     */
    public static final String ITEM_TYPE_IS_COLUMN = "itemTypeIsColumn";
    /**
     * 内部item类型使用
     */
    public static final String ITEM_ITEM_ID_COLUMN = "itemItemIdColumn";
    /**
     * 是否要生成神策json文件，否则直接通过神策系统接收接口地址传输到神策系统
     */
    public static final String IS_GENERATE_LOG = "isGenerateLog";
    /**
     * sa插件前缀
     */
    public static final String PLUGIN = "plugin";
    /**
     * sa插件的扩展名
     */
    public static final String PLUGIN_NAME = "plugin.name";
    /**
     * sa插件的扩展全路径名
     */
    public static final String PLUGIN_CLASS = "plugin.class";
    /**
     * sa插件的扩展参数
     */
    public static final String PLUGIN_PARAM = "plugin.param";

    public static final String USER_NAME = "username";
    public static final String PASSWORD = "password";
    public static final String TABLE_COLUMN_META_DATA = "tableColumnMetaData";
    public static final String MODEL = "model";
    public static final String BATCH_SIZE = "batchSize";
    public static final String UPDATE_WHERE_COLUMN = "updateWhereColumn";
}