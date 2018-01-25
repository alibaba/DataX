package com.alibaba.datax.plugin.reader.mongodbreader;

/**
 * Created by jianying.wcj on 2015/3/17 0017.
 */
public class KeyConstant {
    /**
     * 数组类型
     */
    public static final String ARRAY_TYPE = "array";
    /**
     * 嵌入文档数组类型
     */
    public static final String DOCUMENT_ARRAY_TYPE = "document.array";
    /**
     * 嵌入文档类型
     */
    public static final String DOCUMENT_TYPE = "document";
    /**
     * mongodb 的 host 地址
     */
    public static final String MONGO_ADDRESS = "address";
    /**
     * mongodb 的用户名
     */
    public static final String MONGO_USER_NAME = "userName";
    public static final String MONGO_USERNAME = "username";
    /**
     * mongodb 密码
     */
    public static final String MONGO_USER_PASSWORD = "userPassword";
    public static final String MONGO_PASSWORD = "password";
    /**
     * mongodb 数据库名
     */
    public static final String MONGO_DB_NAME = "dbName";
    public static final String MONGO_DATABASE = "database";
    public static final String MONGO_AUTHDB = "authDb";
    /**
     * mongodb 集合名
     */
    public static final String MONGO_COLLECTION_NAME = "collectionName";
    /**
     * mongodb 查询条件
     */
    public static final String MONGO_QUERY = "query";
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
     * 列分隔符
     */
    public static final String COLUMN_SPLITTER = "splitter";
    /**
     * 跳过的列数
     */
    public static final String SKIP_COUNT = "skipCount";


    public static final String LOWER_BOUND = "lowerBound";
    public static final String UPPER_BOUND = "upperBound";
    public static final String IS_OBJECTID = "isObjectId";
    /**
     * 批量获取的记录数
     */
    public static final String BATCH_SIZE = "batchSize";
    /**
     * MongoDB的_id
     */
    public static final String MONGO_PRIMARY_ID = "_id";
    /**
     * MongoDB的错误码
     */
    public static final int MONGO_UNAUTHORIZED_ERR_CODE = 13;
    public static final int MONGO_ILLEGALOP_ERR_CODE = 20;
    /**
     * 判断是否为数组类型
     * @param type 数据类型
     * @return
     */
    public static boolean isArrayType(String type) {
        return ARRAY_TYPE.equals(type) || DOCUMENT_ARRAY_TYPE.equals(type);
    }

    public static boolean isDocumentType(String type) {
        return type.startsWith(DOCUMENT_TYPE);
    }
}
