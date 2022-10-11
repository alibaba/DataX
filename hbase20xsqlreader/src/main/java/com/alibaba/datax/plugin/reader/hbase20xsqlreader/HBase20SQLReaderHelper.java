package com.alibaba.datax.plugin.reader.hbase20xsqlreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.RdbmsRangeSplitWrap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class HBase20SQLReaderHelper {
    private static final Logger LOG = LoggerFactory.getLogger(HBase20SQLReaderHelper.class);

    private Configuration configuration;

    private Connection connection;
    private List<String> querySql;
    private String fullTableName;
    private List<String> columnNames;
    private String splitKey;
    private List<Object> splitPoints;


    public HBase20SQLReaderHelper (Configuration configuration) {
        this.configuration = configuration;
    }
    /**
     *  校验配置参数是否正确
     */
    public void validateParameter() {
        // queryserver地址必须配置
        String queryServerAddress = configuration.getNecessaryValue(Key.QUERYSERVER_ADDRESS,
                HBase20xSQLReaderErrorCode.REQUIRED_VALUE);
        String serialization = configuration.getString(Key.SERIALIZATION_NAME, Constant.DEFAULT_SERIALIZATION);
        connection = getConnection(queryServerAddress, serialization);

        //判断querySql是否配置，如果配置则table配置可为空，否则table必须配置
        querySql = configuration.getList(Key.QUERY_SQL, String.class);
        if (querySql == null || querySql.isEmpty()) {
            LOG.info("Split according to splitKey or split points.");

            String schema = configuration.getString(Key.SCHEMA, null);
            String tableName = configuration.getNecessaryValue(Key.TABLE, HBase20xSQLReaderErrorCode.REQUIRED_VALUE);
            if (schema != null && !schema.isEmpty()) {
                fullTableName = "\"" + schema + "\".\"" + tableName + "\"";
            } else {
                fullTableName = "\"" + tableName + "\"";
            }
            // 如果列名未配置，默认读取全部列*
            columnNames = configuration.getList(Key.COLUMN, String.class);
            splitKey = configuration.getString(Key.SPLIT_KEY, null);
            splitPoints = configuration.getList(Key.SPLIT_POINT);
            checkTable(schema, tableName);
            dealWhere();
        } else {
            // 用户指定querySql，切分不做处理，根据给定sql读取数据即可
            LOG.info("Split according to query sql.");
        }
    }

    public Connection getConnection(String queryServerAddress, String serialization) {
        String url = String.format(Constant.CONNECT_STRING_TEMPLATE, queryServerAddress, serialization);
        LOG.debug("Connecting to QueryServer [" + url + "] ...");
        Connection conn;
        try {
            Class.forName(Constant.CONNECT_DRIVER_STRING);
            conn = DriverManager.getConnection(url);
            conn.setAutoCommit(false);
        } catch (Throwable e) {
            throw DataXException.asDataXException(HBase20xSQLReaderErrorCode.GET_QUERYSERVER_CONNECTION_ERROR,
                    "无法连接QueryServer，配置不正确或服务未启动，请检查配置和服务状态或者联系HBase管理员.", e);
        }
        LOG.debug("Connected to QueryServer successfully.");
        return conn;
    }

    /**
     * 检查表名、列名和切分列是否存在
     */
    public void checkTable(String schema, String tableName) {
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = connection.createStatement();
            String selectSql = String.format(Constant.SELECT_COLUMNS_TEMPLATE, tableName);

            // 处理schema不为空情况
            if (schema == null || schema.isEmpty()) {
                selectSql = selectSql + " AND TABLE_SCHEM IS NULL";
            } else {
                selectSql = selectSql + " AND TABLE_SCHEM = '" + schema + "'";
            }
            resultSet = statement.executeQuery(selectSql);
            List<String> primaryColumnNames = new ArrayList<String>();
            List<String> allColumnName = new ArrayList<String>();
            while (resultSet.next()) {
                String columnName = resultSet.getString(1);
                allColumnName.add(columnName);
                // 列族为空表示该列为主键列
                if (resultSet.getString(2) == null) {
                    primaryColumnNames.add(columnName);
                }
            }
            if (columnNames != null && !columnNames.isEmpty()) {
                for (String columnName : columnNames) {
                    if (!allColumnName.contains(columnName)) {
                        // 用户配置的列名在元数据中不存在
                        throw DataXException.asDataXException(HBase20xSQLReaderErrorCode.ILLEGAL_VALUE,
                                "您配置的列" + columnName + "在表" + tableName + "的元数据中不存在，请检查您的配置或者联系HBase管理员.");
                    }
                }
            } else {
                columnNames = allColumnName;
                configuration.set(Key.COLUMN, allColumnName);
            }
            if (splitKey != null) {
                // 切分列必须是主键列，否则会严重影响读取性能
                if (!primaryColumnNames.contains(splitKey)) {
                    throw DataXException.asDataXException(HBase20xSQLReaderErrorCode.ILLEGAL_VALUE,
                            "您配置的切分列" + splitKey + "不是表" + tableName + "的主键，请检查您的配置或者联系HBase管理员.");
                }
            }

        } catch (SQLException e) {
            throw DataXException.asDataXException(HBase20xSQLReaderErrorCode.GET_PHOENIX_TABLE_ERROR,
                    "获取表" + tableName + "信息失败，请检查您的集群和表状态或者联系HBase管理员.", e);

        } finally {
            closeJdbc(null, statement, resultSet);
        }
    }

    public void closeJdbc(Connection connection, Statement statement, ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            LOG.warn("数据库连接关闭异常.", HBase20xSQLReaderErrorCode.CLOSE_PHOENIX_CONNECTION_ERROR, e);
        }
    }

    public void dealWhere() {
        String where = configuration.getString(Key.WHERE, null);
        if(StringUtils.isNotBlank(where)) {
            String whereImprove = where.trim();
            if(whereImprove.endsWith(";") || whereImprove.endsWith("；")) {
                whereImprove = whereImprove.substring(0,whereImprove.length()-1);
            }
            configuration.set(Key.WHERE, whereImprove);
        }
    }

    /**
     * 对表进行切分
     */
    public List<Configuration> doSplit(int adviceNumber) {
        List<Configuration> pluginParams = new ArrayList<Configuration>();
        List<String> rangeList;
        String where = configuration.getString(Key.WHERE);
        boolean hasWhere = StringUtils.isNotBlank(where);
        if (querySql == null || querySql.isEmpty()) {
            // 如果splitPoints为空，则根据splitKey自动切分，不过这种切分方式无法保证数据均分，且只支持整形和字符型列
            if (splitPoints == null || splitPoints.isEmpty()) {
                LOG.info("Split according min and max value of splitColumn...");
                Pair<Object, Object> minMaxPK = getPkRange(configuration);
                if (null == minMaxPK) {
                    throw DataXException.asDataXException(HBase20xSQLReaderErrorCode.ILLEGAL_SPLIT_PK,
                            "根据切分主键切分表失败. DataX仅支持切分主键为一个,并且类型为整数或者字符串类型. " +
                                    "请尝试使用其他的切分主键或者联系 HBase管理员 进行处理.");
                }
                if (null == minMaxPK.getLeft() || null == minMaxPK.getRight()) {
                    // 切分后获取到的start/end 有 Null 的情况
                    pluginParams.add(configuration);
                    return pluginParams;
                }
                boolean isStringType = Constant.PK_TYPE_STRING.equals(configuration
                        .getString(Constant.PK_TYPE));
                boolean isLongType = Constant.PK_TYPE_LONG.equals(configuration
                        .getString(Constant.PK_TYPE));
                if (isStringType) {
                    rangeList = RdbmsRangeSplitWrap.splitAndWrap(
                            String.valueOf(minMaxPK.getLeft()),
                            String.valueOf(minMaxPK.getRight()), adviceNumber,
                            splitKey, "'", null);
                } else if (isLongType) {
                    rangeList = RdbmsRangeSplitWrap.splitAndWrap(
                            new BigInteger(minMaxPK.getLeft().toString()),
                            new BigInteger(minMaxPK.getRight().toString()),
                            adviceNumber, splitKey);
                } else {
                    throw DataXException.asDataXException(HBase20xSQLReaderErrorCode.ILLEGAL_SPLIT_PK,
                            "您配置的切分主键(splitPk) 类型 DataX 不支持. DataX 仅支持切分主键为一个,并且类型为整数或者字符串类型. " +
                                    "请尝试使用其他的切分主键或者联系HBase管理员进行处理.");
                }

            } else {
                LOG.info("Split according splitPoints...");
                // 根据指定splitPoints进行切分
                rangeList = buildSplitRange();
            }
            String tempQuerySql;
            if (null != rangeList && !rangeList.isEmpty()) {
                for (String range : rangeList) {
                    Configuration tempConfig = configuration.clone();

                    tempQuerySql = buildQuerySql(columnNames, fullTableName, where)
                            + (hasWhere ? " and " : " where ") + range;
                    LOG.info("Query SQL: " + tempQuerySql);
                    tempConfig.set(Constant.QUERY_SQL_PER_SPLIT, tempQuerySql);
                    pluginParams.add(tempConfig);
                }
            } else {
                Configuration tempConfig = configuration.clone();
                tempQuerySql = buildQuerySql(columnNames, fullTableName, where)
                        + (hasWhere ? " and " : " where ")
                        + String.format(" %s IS NOT NULL", splitKey);
                LOG.info("Query SQL: " + tempQuerySql);
                tempConfig.set(Constant.QUERY_SQL_PER_SPLIT, tempQuerySql);
                pluginParams.add(tempConfig);
            }
        } else {
            // 指定querySql不需要切分
            for (String sql : querySql) {
                Configuration tempConfig = configuration.clone();
                tempConfig.set(Constant.QUERY_SQL_PER_SPLIT, sql);
                pluginParams.add(tempConfig);
            }
        }
        return pluginParams;
    }

    public static String buildQuerySql(List<String> columnNames, String table,
                                       String where) {
        String querySql;
        StringBuilder columnBuilder = new StringBuilder();
        for (String columnName : columnNames) {
            columnBuilder.append("\"").append(columnName).append("\",");
        }
        columnBuilder.setLength(columnBuilder.length() -1);
        if (StringUtils.isBlank(where)) {
            querySql = String.format(Constant.QUERY_SQL_TEMPLATE_WITHOUT_WHERE,
                    columnBuilder.toString(), table);
        } else {
            querySql = String.format(Constant.QUERY_SQL_TEMPLATE, columnBuilder.toString(),
                    table, where);
        }
        return querySql;
    }

    private List<String> buildSplitRange() {
        String getSplitKeyTypeSQL = String.format(Constant.QUERY_COLUMN_TYPE_TEMPLATE, splitKey, fullTableName);
        Statement statement = null;
        ResultSet resultSet = null;
        List<String> splitConditions = new ArrayList<String>();

        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(getSplitKeyTypeSQL);
            ResultSetMetaData rsMetaData = resultSet.getMetaData();
            int type = rsMetaData.getColumnType(1);
            String symbol = "%s";
            switch (type) {
                case Types.CHAR:
                case Types.VARCHAR:
                    symbol = "'%s'";
                    break;
                case Types.DATE:
                    symbol = "TO_DATE('%s')";
                    break;
                case Types.TIME:
                    symbol = "TO_TIME('%s')";
                    break;
                case Types.TIMESTAMP:
                    symbol = "TO_TIMESTAMP('%s')";
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.ARRAY:
                    throw DataXException.asDataXException(HBase20xSQLReaderErrorCode.ILLEGAL_SPLIT_PK,
                            "切分列类型为" + rsMetaData.getColumnTypeName(1) + "，暂不支持该类型字段作为切分列。");
            }
            String splitCondition = null;
            for (int i = 0; i <= splitPoints.size(); i++) {
                if (i == 0) {
                    splitCondition = splitKey + " <= " + String.format(symbol, splitPoints.get(i));
                } else if (i == splitPoints.size()) {
                    splitCondition = splitKey + " > " + String.format(symbol, splitPoints.get(i - 1));
                } else {
                    splitCondition = splitKey + " > " + String.format(symbol, splitPoints.get(i - 1)) +
                            " AND " + splitKey + " <= " + String.format(symbol, splitPoints.get(i));
                }
                splitConditions.add(splitCondition);
            }

            return splitConditions;
        } catch (SQLException e) {
            throw DataXException.asDataXException(HBase20xSQLReaderErrorCode.GET_TABLE_COLUMNTYPE_ERROR,
                    "获取切分列类型失败，请检查服务或给定表和切分列是否正常，或者联系HBase管理员进行处理。", e);
        } finally {
            closeJdbc(null, statement, resultSet);
        }

    }

    private Pair<Object, Object> getPkRange(Configuration configuration) {
        String pkRangeSQL = String.format(Constant.QUERY_MIN_MAX_TEMPLATE, splitKey, splitKey, fullTableName);
        String where = configuration.getString(Key.WHERE);
        if (StringUtils.isNotBlank(where)) {
            pkRangeSQL = String.format("%s WHERE (%s AND %s IS NOT NULL)",
                    pkRangeSQL, where, splitKey);
        }
        Statement statement = null;
        ResultSet resultSet = null;
        Pair<Object, Object> minMaxPK = null;

        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(pkRangeSQL);
            ResultSetMetaData rsMetaData = resultSet.getMetaData();

            if (isPKTypeValid(rsMetaData)) {
                if (isStringType(rsMetaData.getColumnType(1))) {
                    if(configuration != null) {
                        configuration
                                .set(Constant.PK_TYPE, Constant.PK_TYPE_STRING);
                    }
                    if (resultSet.next()) {
                        minMaxPK = new ImmutablePair<Object, Object>(
                                resultSet.getString(1), resultSet.getString(2));
                    }
                } else if (isLongType(rsMetaData.getColumnType(1))) {
                    if(configuration != null) {
                        configuration.set(Constant.PK_TYPE, Constant.PK_TYPE_LONG);
                    }
                    if (resultSet.next()) {
                        minMaxPK = new ImmutablePair<Object, Object>(
                                resultSet.getLong(1), resultSet.getLong(2));
                    }
                } else {
                    throw DataXException.asDataXException(HBase20xSQLReaderErrorCode.ILLEGAL_SPLIT_PK,
                            "您配置的DataX切分主键(splitPk)有误. 因为您配置的切分主键(splitPk) 类型 DataX 不支持. " +
                                    "DataX 仅支持切分主键为一个,并且类型为整数或者字符串类型. 请尝试使用其他的切分主键或者联系HBASE管理员进行处理.");
                }
            } else {
                throw DataXException.asDataXException(HBase20xSQLReaderErrorCode.ILLEGAL_SPLIT_PK,
                        "您配置的DataX切分主键(splitPk)有误. 因为您配置的切分主键(splitPk) 类型 DataX 不支持. " +
                                "DataX 仅支持切分主键为一个,并且类型为整数或者字符串类型. 请尝试使用其他的切分主键或者联系HBASE管理员进行处理.");
            }
        } catch (SQLException e) {
            throw DataXException.asDataXException(HBase20xSQLReaderErrorCode.ILLEGAL_SPLIT_PK, e);
        } finally {
            closeJdbc(null, statement, resultSet);
        }

        return minMaxPK;
    }

    private static boolean isPKTypeValid(ResultSetMetaData rsMetaData) {
        boolean ret = false;
        try {
            int minType = rsMetaData.getColumnType(1);
            int maxType = rsMetaData.getColumnType(2);

            boolean isNumberType = isLongType(minType);

            boolean isStringType = isStringType(minType);

            if (minType == maxType && (isNumberType || isStringType)) {
                ret = true;
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                    "DataX获取切分主键(splitPk)字段类型失败. 该错误通常是系统底层异常导致. 请联系旺旺:askdatax或者DBA处理.");
        }
        return ret;
    }

    private static boolean isLongType(int type) {
        boolean isValidLongType = type == Types.BIGINT || type == Types.INTEGER
                || type == Types.SMALLINT || type == Types.TINYINT;
        return isValidLongType;
    }

    private static boolean isStringType(int type) {
        return type == Types.CHAR || type == Types.NCHAR
                || type == Types.VARCHAR || type == Types.LONGVARCHAR
                || type == Types.NVARCHAR;
    }
}
