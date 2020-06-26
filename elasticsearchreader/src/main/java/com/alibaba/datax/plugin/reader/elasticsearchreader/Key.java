package com.alibaba.datax.plugin.reader.elasticsearchreader;


public final class Key {

    /*
     * @name:  esClusterName
     * @description:  elastic search cluster name
     */
    public final static String esClusterName = "esClusterName";

    /*
     * @name:  esClusterIP
     * @description:  elastic search cluster address
     */
    public final static String esClusterAddress = "esClusterAddress";




    public static final String esColumn = "column";


    public static final String COLUMN_NAME = "name";

    public final static String esUsername = "username";


    public final static String esPassword = "password";
    public final static String esBatchSize = "batchSize";

    public final static String keepAlive = "keepAlive";


    /*
     * @name: esIndex
     * @description:  elastic search index
     */
    public final static String esIndex = "esIndex";

    /*
     * @name: esType
     * @description:  elastic search type
     */
    public final static String esType = "esType";


    public final static String query = "query";

    /*
     * @name: batchSize
     * @description: elasticsearch batch size
     */
    public final static String batchSize = "batchSize";

    public final static String splitPk = "splitPk";

    /**
     * 切片索引
     */
    public final static String splitIndex = "splitIndex";
    /**
     * 切片数
     */
    public final static String splitNum = "splitNum";


    public static  String timeout = "timeout";

    public static final String pathPrefix = "pathPrefix";


}
