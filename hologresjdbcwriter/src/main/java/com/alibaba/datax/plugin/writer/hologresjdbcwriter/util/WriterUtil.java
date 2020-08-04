package com.alibaba.datax.plugin.writer.hologresjdbcwriter.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.druid.sql.parser.ParserException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class WriterUtil {
    private static final Logger LOG = LoggerFactory.getLogger(WriterUtil.class);

    //TODO 切分报错
    public static List<Configuration> doSplit(Configuration simplifiedConf,
                                              int adviceNumber) {

        List<Configuration> splitResultConfigs = new ArrayList<Configuration>();

        int tableNumber = simplifiedConf.getInt(Constant.TABLE_NUMBER_MARK);

        //处理单表的情况
        if (tableNumber == 1) {
            //由于在之前的  master prepare 中已经把 table,jdbcUrl 提取出来，所以这里处理十分简单
            for (int j = 0; j < adviceNumber; j++) {
                splitResultConfigs.add(simplifiedConf.clone());
            }

            return splitResultConfigs;
        }

        if (tableNumber != adviceNumber) {
            throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                    String.format("您的配置文件中的列配置信息有误. 您要写入的目的端的表个数是:%s , 但是根据系统建议需要切分的份数是：%s. 请检查您的配置并作出修改.",
                            tableNumber, adviceNumber));
        }

        String jdbcUrl;
        List<String> preSqls = simplifiedConf.getList(Key.PRE_SQL, String.class);
        List<String> postSqls = simplifiedConf.getList(Key.POST_SQL, String.class);

        List<Object> conns = simplifiedConf.getList(Constant.CONN_MARK,
                Object.class);

        for (Object conn : conns) {
            Configuration sliceConfig = simplifiedConf.clone();

            Configuration connConf = Configuration.from(conn.toString());
            jdbcUrl = connConf.getString(Key.JDBC_URL);
            sliceConfig.set(Key.JDBC_URL, jdbcUrl);

            sliceConfig.remove(Constant.CONN_MARK);

            List<String> tables = connConf.getList(Key.TABLE, String.class);

            for (String table : tables) {
                Configuration tempSlice = sliceConfig.clone();
                tempSlice.set(Key.TABLE, table);
                tempSlice.set(Key.PRE_SQL, renderPreOrPostSqls(preSqls, table));
                tempSlice.set(Key.POST_SQL, renderPreOrPostSqls(postSqls, table));

                splitResultConfigs.add(tempSlice);
            }

        }

        return splitResultConfigs;
    }

    public static List<String> renderPreOrPostSqls(List<String> preOrPostSqls, String tableName) {
        if (null == preOrPostSqls) {
            return Collections.emptyList();
        }

        List<String> renderedSqls = new ArrayList<String>();
        for (String sql : preOrPostSqls) {
            //preSql为空时，不加入执行队列
            if (StringUtils.isNotBlank(sql)) {
                renderedSqls.add(sql.replace(Constant.TABLE_NAME_PLACEHOLDER, tableName));
            }
        }

        return renderedSqls;
    }

    public static void executeSqls(Connection conn, List<String> sqls, String basicMessage,DataBaseType dataBaseType) {
        Statement stmt = null;
        String currentSql = null;
        try {
            stmt = conn.createStatement();
            for (String sql : sqls) {
                currentSql = sql;
                DBUtil.executeSqlWithoutResultSet(stmt, sql);
            }
        } catch (Exception e) {
            throw RdbmsException.asQueryException(dataBaseType,e,currentSql,null,null);
        } finally {
            DBUtil.closeDBResources(null, stmt, null);
        }
    }

    public static String getWriteTemplate(List<String> fieldNames, List<String>pkNames,String writeMode, int batchSize) {
        boolean isWriteModeLegal = writeMode.trim().toLowerCase().startsWith("insert")
                || writeMode.trim().toLowerCase().startsWith("ignore")
                || writeMode.trim().toLowerCase().startsWith("update");

        if (!isWriteModeLegal) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                    String.format("您所配置的 writeMode:%s 错误. 因为hologres只支持 ignore,update 或 insert 方式. 请检查您的配置并作出修改.", writeMode));
        }
        if (writeMode.trim().toLowerCase().startsWith("insert")&&pkNames!= null){
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                    String.format("您所配置的 writeMode:%s 错误. 因为 insert方式下，不能有主键。有主键时，请采用update 或 ignore方式.", writeMode));
        }
        if ((writeMode.trim().toLowerCase().startsWith("ignore")
                || writeMode.trim().toLowerCase().startsWith("update"))&& null == pkNames){
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                    String.format("您所配置的 writeMode:%s 错误. 因为update 或 ignore方式下，需要主键.", writeMode));
        }
        String[] colnames = new String[fieldNames.size()];
        String[] excludedExpress;

        if (null != pkNames && pkNames.size() != 0) {
            excludedExpress = new String[fieldNames.size() - pkNames.size()];
        } else {
            excludedExpress = new String[fieldNames.size()];
        }

        int index = 0;
        for (int i = 0; i < colnames.length; ++i) {
            colnames[i] = "\"" + fieldNames.get(i) + "\"";
            if ((writeMode.trim().toLowerCase().startsWith("update")) && (null == pkNames || !pkNames.contains(fieldNames.get(i)))) {
                excludedExpress[index++] = colnames[i] + "= EXCLUDED." + colnames[i] + "";
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO %s (");
        sb.append(StringUtils.join(colnames, ","));
        sb.append(") values ");
        sb.append(createPreparePlaceholder(batchSize, fieldNames.size()));
        if (writeMode.trim().toLowerCase().startsWith("insert")) {
            return sb.toString();
        } else {
            String[] pkFieldWithDQM = new String[pkNames.size()];
            index = 0;
            for (String s : pkNames) {
                pkFieldWithDQM[index++] = "\"" + s + "\"";
            }
            sb.append(" ON CONFLICT (");
            sb.append(StringUtils.join(pkFieldWithDQM, ","));
            if (writeMode.trim().toLowerCase().startsWith("ignore")) {
                sb.append(") DO NOTHING");
                return sb.toString();
            } else {
                sb.append(") DO UPDATE SET ");
                sb.append(StringUtils.join(excludedExpress, ","));
                return sb.toString();
            }
        }
    }

    private static String createPreparePlaceholder(int batchSize, int fieldNum) {
        if (batchSize < 1 || fieldNum < 1) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("(?");
        for (int i = 0; i < fieldNum - 1; ++i) {
            sb.append(",?");
        }
        sb.append(")");
        String placeholders = sb.toString();
        for (int i = 0; i < batchSize - 1; ++i) {
            sb.append("," + placeholders);
        }
        return sb.toString();
    }

    public static void preCheckPrePareSQL(Configuration originalConfig, DataBaseType type) {
        List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
        Configuration connConf = Configuration.from(conns.get(0).toString());
        String table = connConf.getList(Key.TABLE, String.class).get(0);

        List<String> preSqls = originalConfig.getList(Key.PRE_SQL,
                String.class);
        List<String> renderedPreSqls = WriterUtil.renderPreOrPostSqls(
                preSqls, table);

        if (null != renderedPreSqls && !renderedPreSqls.isEmpty()) {
            LOG.info("Begin to preCheck preSqls:[{}].",
                    StringUtils.join(renderedPreSqls, ";"));
            for(String sql : renderedPreSqls) {
                try{
                    DBUtil.sqlValid(sql, type);
                }catch(ParserException e) {
                    throw RdbmsException.asPreSQLParserException(type,e,sql);
                }
            }
        }
    }

    public static void preCheckPostSQL(Configuration originalConfig, DataBaseType type) {
        List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
        Configuration connConf = Configuration.from(conns.get(0).toString());
        String table = connConf.getList(Key.TABLE, String.class).get(0);

        List<String> postSqls = originalConfig.getList(Key.POST_SQL,
                String.class);
        List<String> renderedPostSqls = WriterUtil.renderPreOrPostSqls(
                postSqls, table);
        if (null != renderedPostSqls && !renderedPostSqls.isEmpty()) {

            LOG.info("Begin to preCheck postSqls:[{}].",
                    StringUtils.join(renderedPostSqls, ";"));
            for(String sql : renderedPostSqls) {
                try{
                    DBUtil.sqlValid(sql, type);
                }catch(ParserException e){
                    throw RdbmsException.asPostSQLParserException(type,e,sql);
                }

            }
        }
    }


}
