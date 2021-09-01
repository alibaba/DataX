package com.starrocks.connector.datax.plugin.writer.starrockswriter.util;

import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.druid.sql.parser.ParserException;
import com.starrocks.connector.datax.plugin.writer.starrockswriter.StarRocksWriterOptions;
import com.google.common.base.Strings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public final class StarRocksWriterUtil {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksWriterUtil.class);

    private StarRocksWriterUtil() {}

    public static List<String> getStarRocksColumns(Connection conn, String databaseName, String tableName) {
        String currentSql = String.format("SELECT COLUMN_NAME FROM `information_schema`.`COLUMNS` WHERE `TABLE_SCHEMA` = '%s' AND `TABLE_NAME` = '%s' ORDER BY `ORDINAL_POSITION` ASC;", databaseName, tableName);
        List<String> columns = new ArrayList<>();
        ResultSet rs = null;
        try {
            rs = DBUtil.query(conn, currentSql);
            while (DBUtil.asyncResultSetNext(rs)) {
                String colName = rs.getString("COLUMN_NAME");
                columns.add(colName);
            }
            return columns;
        } catch (Exception e) {
            throw RdbmsException.asQueryException(DataBaseType.MySql, e, currentSql, null, null);
        } finally {
            DBUtil.closeDBResources(rs, null, null);
        }
    }

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

    public static void preCheckPrePareSQL(StarRocksWriterOptions options) {
        String table = options.getTable();
        List<String> preSqls = options.getPreSqlList();
        List<String> renderedPreSqls = StarRocksWriterUtil.renderPreOrPostSqls(preSqls, table);
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

    public static void preCheckPostSQL(StarRocksWriterOptions options) {
        String table = options.getTable();
        List<String> postSqls = options.getPostSqlList();
        List<String> renderedPostSqls = StarRocksWriterUtil.renderPreOrPostSqls(postSqls, table);
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
