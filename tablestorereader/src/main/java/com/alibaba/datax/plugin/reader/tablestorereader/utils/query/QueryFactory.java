package com.alibaba.datax.plugin.reader.tablestorereader.utils.query;

import com.alibaba.datax.common.util.Configuration;
import com.alicloud.openservices.tablestore.model.search.query.Query;

public class QueryFactory {

    private static String CLASS = "class";
    private static String JSONPARMAS = "jsonParams";
    private static String DEFAULT_CLASS = "MatchAllQuery";

    /**
     * 构建Query
     * <p>
     * todo 构建过程可使用类路劲加载，避免switch硬编码
     *
     * @param param
     * @return
     * @throws Exception
     */
    public static Query build(Configuration param) throws Exception {

        String aClass = param.getString(CLASS);

        if (null == aClass) {
            aClass = DEFAULT_CLASS;
        }

        QueryBuilder builder = null;

        switch (aClass.toUpperCase()) {
            case "TERMQUERY":
                builder = new TermQueryBuilder();
                break;
            case "MATCHALLQUERY":
                builder = new MatchAllQueryBuilder();
                break;
        }

        if (null == builder) {
            throw new NoQueryBuilderFoundException("class :" + aClass + "not found");
        }

        return builder.build(param.getConfiguration(JSONPARMAS));
    }
}
