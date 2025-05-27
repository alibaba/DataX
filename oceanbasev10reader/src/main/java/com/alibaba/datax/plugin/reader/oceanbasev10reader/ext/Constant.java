package com.alibaba.datax.plugin.reader.oceanbasev10reader.ext;

/**
 * @author johnrobbet
 */
public class Constant {

    public static String WEAK_READ_QUERY_SQL_TEMPLATE_WITHOUT_WHERE = "select /*+read_consistency(weak)*/ %s from %s ";

    public static String WEAK_READ_QUERY_SQL_TEMPLATE = "select /*+read_consistency(weak)*/ %s from %s where (%s)";
}
