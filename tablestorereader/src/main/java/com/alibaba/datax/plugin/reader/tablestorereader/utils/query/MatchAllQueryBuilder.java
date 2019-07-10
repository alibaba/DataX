package com.alibaba.datax.plugin.reader.tablestorereader.utils.query;

import com.alibaba.datax.common.util.Configuration;
import com.alicloud.openservices.tablestore.model.search.query.MatchAllQuery;

/**
 * 构建查询全部
 */
public class MatchAllQueryBuilder implements QueryBuilder<MatchAllQuery> {

    @Override
    public MatchAllQuery build(Configuration configuration) {
        return new MatchAllQuery();
    }
}

