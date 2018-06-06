package com.alibaba.datax.plugin.writer.sequoiadbwriter;

/**
 * @author jianhong xie
 * @Date 2018-05-11
 */
public class KeyConstant {

    /**
     * SequoiaDB 的host地址
     */
    public static final String SDB_ADDRESS = "address";

    /**
     * SequoiaDB的用户名
     */
    public static final String SDB_USER_NAME = "userName";

    /**
     * SequoiaDB的密码
     */
    public static final String SDB_USER_PASSWORD = "userPassword";

    /**
     * SequoiaDB的列
     */
    public static final String SDB_COLUMN = "column";

    /**
     * SequoiaDB的集合空间
     */
    public static final String SDB_COLLECTION_SPACE = "collectionSpaceName";

    /**
     * SequoiaDB的集合
     */
    public static final String SDB_COLLECTION = "collectionName";

    /**
     * 列的类型
     */
    public static final String COLUMN_TYPE = "type";

    /**
     * 数组类型
     */
    public static final String ARRAY_TYPE = "array";

    /**
     * ObjectId类型
     */
    public static final String OBJECT_ID_TYPE = "objectId";

    /**
     * Object类型
     */
    public static final String DOCUMENT_TYPE = "object";

    /**
     * 列名
     */
    public static final String COLUMN_NAME = "name" ;
    /**
     * 列分隔符
     */
    public static final String COLUMN_SPLITTER = "splitter";
    /**
     * 数组中每个元素的类型
     */
    public static final String ITEM_TYPE = "itemType";


    /**
     * 判断是否为数组类型
     * @param type 数据类型
     * @return
     */
    public static boolean isArrayType(String type) {
        return ARRAY_TYPE.equalsIgnoreCase(type);
    }
    /**
     * 判断是否为ObjectId类型
     * @param type 数据类型
     * @return
     */
    public static boolean isObjectIdType(String type) {
        return OBJECT_ID_TYPE.equalsIgnoreCase(type);
    }

    /**
     * 判断是否为Document类型
     * @param type 数据类型
     * @return
     */
    public static boolean isDocumentType(String type) {
        return DOCUMENT_TYPE.equalsIgnoreCase(type);
    }
}
