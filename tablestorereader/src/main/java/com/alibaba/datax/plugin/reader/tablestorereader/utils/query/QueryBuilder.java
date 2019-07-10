package com.alibaba.datax.plugin.reader.tablestorereader.utils.query;

import com.alibaba.datax.common.util.Configuration;
import com.alicloud.openservices.tablestore.model.search.query.Query;

/**
 * 构建不同的查询query的类接口
 *
 * @param <T>
 */
public interface QueryBuilder<T extends Query> {

    /**
     * 构建query类

     * @param configuration
     * @return
     */
    T build(Configuration configuration) throws Exception;
}
