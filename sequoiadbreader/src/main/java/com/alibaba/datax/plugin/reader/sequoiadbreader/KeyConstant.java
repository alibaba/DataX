package com.alibaba.datax.plugin.reader.sequoiadbreader;

public class KeyConstant {

    /**
     * 数组类型
     */
    public static final String ARRAY_TYPE = "array";

    /**
     * sequoiadb 的 host 地址
     */
    public static final String SDB_ADDRESS = "address";
    /**
     * sequoiadb 的用户名
     */
    public static final String SDB_USERNAME = "userName";
    /**
     * sequoiadb 密码
     */
    public static final String SDB_PASSWORD = "userPassword";
    /**
     * sequoiadb 集合名
     */
    public static final String SDB_COLLECTION_NAME = "collectionName";

    /**
     * sequoiadb 集合空间名
     */
    public static final String SDB_COLLECTION_SPACE_NAME = "collectionSpaceName";

    /**
     * sequoiadb 的列
     */
    public static final String SDB_COLUMN = "column";
    /**
     * 每个列的名字
     */
    public static final String COLUMN_NAME = "name";
    /**
     * 每个列的类型
     */
    public static final String COLUMN_TYPE = "type";
    /**
     * 列分隔符
     */
    public static final String COLUMN_SPLITTER = "splitter";
    public static final String LOWER_BOUND = "lowerBound";
    public static final String UPPER_BOUND = "upperBound";


    /**
     * 判断是否为数组类型
     * @param type 数据类型
     * @return
     */
    public static boolean isArrayType(String type) {
        return ARRAY_TYPE.equalsIgnoreCase(type);
    }
}
