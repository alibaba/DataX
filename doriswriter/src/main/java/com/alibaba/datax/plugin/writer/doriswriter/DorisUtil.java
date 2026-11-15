package com.alibaba.datax.plugin.writer.doriswriter;

import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.fastjson2.JSON;
import com.google.common.base.Strings;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

/**
 * jdbc util
 */
public class DorisUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DorisUtil.class);

    private DorisUtil() {}

    public static List<String> getDorisTableColumns( Connection conn, String databaseName, String tableName) {
        String currentSql = String.format("SELECT COLUMN_NAME FROM `information_schema`.`COLUMNS` WHERE `TABLE_SCHEMA` = '%s' AND `TABLE_NAME` = '%s' ORDER BY `ORDINAL_POSITION` ASC;", databaseName, tableName);
        List<String> columns = new ArrayList<> ();
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
            if (! Strings.isNullOrEmpty(sql)) {
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

    public static void preCheckPrePareSQL( Keys options) {
        String table = options.getTable();
        List<String> preSqls = options.getPreSqlList();
        List<String> renderedPreSqls = DorisUtil.renderPreOrPostSqls(preSqls, table);
        if (null != renderedPreSqls && !renderedPreSqls.isEmpty()) {
            LOG.info("Begin to preCheck preSqls:[{}].", String.join(";", renderedPreSqls));
            for (String sql : renderedPreSqls) {
                try {
                    DBUtil.sqlValid(sql, DataBaseType.MySql);
                } catch ( ParserException e) {
                    throw RdbmsException.asPreSQLParserException(DataBaseType.MySql,e,sql);
                }
            }
        }
    }

    public static void preCheckPostSQL( Keys options) {
        String table = options.getTable();
        List<String> postSqls = options.getPostSqlList();
        List<String> renderedPostSqls = DorisUtil.renderPreOrPostSqls(postSqls, table);
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

    public static <T> T checkNotNull(T reference) {
        if (reference == null) {
            throw new NullPointerException();
        } else {
            return reference;
        }
    }

    public static String getLoadHost(Keys options) throws IOException {
        List<String> hostList = options.getLoadUrlList();
        for (int i = 0; i < hostList.size(); i++) {
            String host = new StringBuilder("http://").append(hostList.get((i))).toString();
            if (checkConnection(host)) {
                return host;
            }
            continue;
        }
        throw new IOException ("load_url cannot be empty, or the host cannot connect.Please check your configuration.");
    }


    private static boolean checkConnection(String host) {
        try {
            URL url = new URL(host);
            HttpURLConnection co = (HttpURLConnection) url.openConnection();
            co.setConnectTimeout(5000);
            co.connect();
            co.disconnect();
            return true;
        } catch (Exception e1) {
            LOG.error("The connection failed, host is {}", host);
            return false;
        }
    }


    public static boolean checkIsStreamLoad(Keys options) {
        final HttpClientBuilder httpClientBuilder = HttpClients
                .custom()
                .disableRedirectHandling();
        try (CloseableHttpClient httpclient = httpClientBuilder.build()) {
            String url = getLoadHost(options) + "/copy/query";
            HttpPost httpPost = new HttpPost(url);
            try (CloseableHttpResponse resp = httpclient.execute(httpPost)) {
                if (resp.getStatusLine().getStatusCode() == 200) {
                    Map<String, Object> result = (Map<String, Object>) JSON.parse(EntityUtils.toString(resp.getEntity()));
                    if (result != null && (int) result.get("code") == 401) {
                        return false;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }


    public static byte[] addRows(Keys options, List<byte[]> rows, int totalBytes) {
        if (Keys.StreamLoadFormat.CSV.equals(options.getStreamLoadFormat())) {
            Map<String, Object> props = (options.getLoadProps() == null ? new HashMap<>() : options.getLoadProps());
            byte[] lineDelimiter = DelimiterParser.parse((String) props.get("line_delimiter"), "\n").getBytes(StandardCharsets.UTF_8);
            ByteBuffer bos = ByteBuffer.allocate(totalBytes + rows.size() * lineDelimiter.length);
            for (byte[] row : rows) {
                bos.put(row);
                bos.put(lineDelimiter);
            }
            return bos.array();
        }

        if (Keys.StreamLoadFormat.JSON.equals(options.getStreamLoadFormat())) {
            ByteBuffer bos = ByteBuffer.allocate(totalBytes + (rows.isEmpty() ? 2 : rows.size() + 1));
            bos.put("[".getBytes(StandardCharsets.UTF_8));
            byte[] jsonDelimiter = ",".getBytes(StandardCharsets.UTF_8);
            boolean isFirstElement = true;
            for (byte[] row : rows) {
                if (!isFirstElement) {
                    bos.put(jsonDelimiter);
                }
                bos.put(row);
                isFirstElement = false;
            }
            bos.put("]".getBytes(StandardCharsets.UTF_8));
            return bos.array();
        }
        throw new RuntimeException("Failed to join rows data, unsupported `format` from stream load properties:");
    }

    public static boolean isNullOrEmpty(Map<?, ?> map) {
        return map == null || map.isEmpty();
    }
}
