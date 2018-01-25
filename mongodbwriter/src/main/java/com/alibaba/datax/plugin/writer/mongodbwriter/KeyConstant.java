package com.alibaba.datax.plugin.writer.mongodbwriter;

public class KeyConstant {
    /**
     * mongodb 的 host 地址
     */
    public static final String MONGO_ADDRESS = "address";
    /**
     * 数组类型
     */
    public static final String ARRAY_TYPE = "array";
    /**
     * ObjectId类型
     */
    public static final String OBJECT_ID_TYPE = "objectid";
    /**
     * mongodb 的用户名
     */
    public static final String MONGO_USER_NAME = "userName";
    /**
     * mongodb 密码
     */
    public static final String MONGO_USER_PASSWORD = "userPassword";
    /**
     * mongodb 数据库名
     */
    public static final String MONGO_DB_NAME = "dbName";
    /**
     * mongodb 集合名
     */
    public static final String MONGO_COLLECTION_NAME = "collectionName";
    /**
     * mongodb 的列
     */
    public static final String MONGO_COLUMN = "column";
    /**
     * 每个列的名字
     */
    public static final String COLUMN_NAME = "name";
    /**
     * 每个列的类型
     */
    public static final String COLUMN_TYPE = "type";
    /**
     * 数组中每个元素的类型
     */
    public static final String ITEM_TYPE = "itemtype";
    /**
     * 列分隔符
     */
    public static final String COLUMN_SPLITTER = "splitter";
    /**
     * 数据更新列信息
     */
    public static final String WRITE_MODE = "writeMode";
    /**
     * 有相同的记录是否覆盖，默认为false
     */
    public static final String IS_REPLACE = "isReplace";
    /**
     * 指定用来判断是否覆盖的 业务主键
     */
    public static final String UNIQUE_KEY = "replaceKey";
    /**
     * 判断是否为数组类型
     * @param type 数据类型
     * @return
     */
    public static boolean isArrayType(String type) {
        return ARRAY_TYPE.equals(type);
    }
    /**
     * 判断是否为ObjectId类型
     * @param type 数据类型
     * @return
     */
    public static boolean isObjectIdType(String type) {
        return OBJECT_ID_TYPE.equals(type);
    }
    /**
     * 判断一个值是否为true
     * @param value
     * @return
     */
    public static boolean isValueTrue(String value){
        return "true".equals(value);
    }
}
