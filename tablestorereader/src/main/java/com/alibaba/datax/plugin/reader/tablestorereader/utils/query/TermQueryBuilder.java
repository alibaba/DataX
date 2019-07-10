package com.alibaba.datax.plugin.reader.tablestorereader.utils.query;

import com.alibaba.datax.common.util.Configuration;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.TermQuery;

public class TermQueryBuilder implements QueryBuilder<TermQuery> {

    private static String TERM = "term";
    private static String TERM_VALUE = "value";
    private static String TERM_TYPE = "type";
    private static String FIELDNAME = "fieldName";

    @Override
    public TermQuery build(Configuration configuration) throws Exception {
        TermQuery termQuery = new TermQuery();

        Configuration term = configuration.getConfiguration(TERM);

        String value = term.getString(TERM_VALUE);
        String type = term.getString(TERM_TYPE);

        ColumnType termColumnType = TableStoreDataTypeUtil.type(type);

        if (null == termColumnType) {
            throw new NoColumnTypeFoundException();
        }

        ColumnValue columnValue = new ColumnValue(value, termColumnType);

        termQuery.setTerm(columnValue);
        termQuery.setFieldName(configuration.getString(FIELDNAME));

        return termQuery;
    }
}
