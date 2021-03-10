package com.dorisdb.connector.datax.plugin.writer.doriswriter.util;

import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.druid.sql.parser.ParserException;
import com.dorisdb.connector.datax.plugin.writer.doriswriter.DorisWriterOptions;
import com.google.common.base.Strings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

public final class DorisWriterUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DorisWriterUtil.class);

    private DorisWriterUtil() {}

    public static List<String> renderPreOrPostSqls(List<String> preOrPostSqls, String tableName) {
        if (null == preOrPostSqls) {
            return Collections.emptyList();
        }
        List<String> renderedSqls = new ArrayList<>();
        for (String sql : preOrPostSqls) {
            if (!Strings.isNullOrEmpty(sql)) {
                renderedSqls.add(sql.replace(Constant.TABLE_NAME_PLACEHOLDER, tableName));
            }
        }
        return renderedSqls;
    }

    public static void executeSqls(Connection conn, List<String> sqls) {
        Statement stmt = null;
        String currentSql = null;
        try {
            stmt = conn.createStatement();
            for (String sql : sqls) {
                currentSql = sql;
                DBUtil.executeSqlWithoutResultSet(stmt, sql);
            }
        } catch (Exception e) {
            throw RdbmsException.asQueryException(DataBaseType.MySql, e, currentSql, null, null);
        } finally {
            DBUtil.closeDBResources(null, stmt, null);
        }
    }

    public static void preCheckPrePareSQL(DorisWriterOptions options) {
        String table = options.getTable();
        List<String> preSqls = options.getPreSqlList();
        List<String> renderedPreSqls = DorisWriterUtil.renderPreOrPostSqls(preSqls, table);
        if (null != renderedPreSqls && !renderedPreSqls.isEmpty()) {
            LOG.info("Begin to preCheck preSqls:[{}].", String.join(";", renderedPreSqls));
            for (String sql : renderedPreSqls) {
                try {
                    DBUtil.sqlValid(sql, DataBaseType.MySql);
                } catch (ParserException e) {
                    throw RdbmsException.asPreSQLParserException(DataBaseType.MySql,e,sql);
                }
            }
        }
    }

    public static void preCheckPostSQL(DorisWriterOptions options) {
        String table = options.getTable();
        List<String> postSqls = options.getPostSqlList();
        List<String> renderedPostSqls = DorisWriterUtil.renderPreOrPostSqls(postSqls, table);
        if (null != renderedPostSqls && !renderedPostSqls.isEmpty()) {
            LOG.info("Begin to preCheck postSqls:[{}].", String.join(";", renderedPostSqls));
            for(String sql : renderedPostSqls) {
                try {
                    DBUtil.sqlValid(sql, DataBaseType.MySql);
                } catch (ParserException e){
                    throw RdbmsException.asPostSQLParserException(DataBaseType.MySql,e,sql);
                }
            }
        }
    }
}
