package com.alibaba.datax.plugin.rdbms.util;

import com.alibaba.datax.common.exception.DataXException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * refer:http://blog.csdn.net/ring0hx/article/details/6152528
 * <p/>
 */
public enum DataBaseType {
    MySql("mysql", "com.mysql.jdbc.Driver"),
    Tddl("mysql", "com.mysql.jdbc.Driver"),
    DRDS("drds", "com.mysql.jdbc.Driver"),
    Oracle("oracle", "oracle.jdbc.OracleDriver"),
    SQLServer("sqlserver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
    PostgreSQL("postgresql", "org.postgresql.Driver"),
    RDBMS("rdbms", "com.alibaba.datax.plugin.rdbms.util.DataBaseType"),
    DB2("db2", "com.ibm.db2.jcc.DB2Driver"),
    ADS("ads","com.mysql.jdbc.Driver"),
    ClickHouse("clickhouse", "ru.yandex.clickhouse.ClickHouseDriver"),
    KingbaseES("kingbasees", "com.kingbase8.Driver"),
    Oscar("oscar", "com.oscar.Driver"),
    OceanBase("oceanbase", "com.alipay.oceanbase.jdbc.Driver");


    private String typeName;
    private String driverClassName;

    DataBaseType(String typeName, String driverClassName) {
        this.typeName = typeName;
        this.driverClassName = driverClassName;
    }

    public String getDriverClassName() {
        return this.driverClassName;
    }

    public String appendJDBCSuffixForReader(String jdbc) {
        String result = jdbc;
        String suffix = null;
        switch (this) {
            case MySql:
            case DRDS:
            case OceanBase:
                suffix = "yearIsDateType=false&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&rewriteBatchedStatements=true";
                if (jdbc.contains("?")) {
                    result = jdbc + "&" + suffix;
                } else {
                    result = jdbc + "?" + suffix;
                }
                break;
            case Oracle:
                break;
            case SQLServer:
                break;
            case DB2:
                break;
            case PostgreSQL:
                break;
            case ClickHouse:
                break;
            case RDBMS:
                break;
            case KingbaseES:
                break;
            case Oscar:
                break;
            default:
                throw DataXException.asDataXException(DBUtilErrorCode.UNSUPPORTED_TYPE, "unsupported database type.");
        }

        return result;
    }

    public String appendJDBCSuffixForWriter(String jdbc) {
        String result = jdbc;
        String suffix = null;
        switch (this) {
            case MySql:
                suffix = "yearIsDateType=false&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true&tinyInt1isBit=false";
                if (jdbc.contains("?")) {
                    result = jdbc + "&" + suffix;
                } else {
                    result = jdbc + "?" + suffix;
                }
                break;
            case DRDS:
                suffix = "yearIsDateType=false&zeroDateTimeBehavior=convertToNull";
                if (jdbc.contains("?")) {
                    result = jdbc + "&" + suffix;
                } else {
                    result = jdbc + "?" + suffix;
                }
                break;
            case Oracle:
                break;
            case SQLServer:
                break;
            case DB2:
                break;
            case PostgreSQL:
                break;
            case ClickHouse:
                break;
            case RDBMS:
                break;
            case KingbaseES:
                break;
            case Oscar:
                break;
            case OceanBase:
                suffix = "yearIsDateType=false&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&rewriteBatchedStatements=true";
                if (jdbc.contains("?")) {
                    result = jdbc + "&" + suffix;
                } else {
                    result = jdbc + "?" + suffix;
                }
                break;
            default:
                throw DataXException.asDataXException(DBUtilErrorCode.UNSUPPORTED_TYPE, "unsupported database type.");
        }

        return result;
    }

    public String formatPk(String splitPk) {
        String result = splitPk;

        switch (this) {
            case MySql:
            case Oracle:
                if (splitPk.length() >= 2 && splitPk.startsWith("`") && splitPk.endsWith("`")) {
                    result = splitPk.substring(1, splitPk.length() - 1).toLowerCase();
                }
                break;
            case SQLServer:
                if (splitPk.length() >= 2 && splitPk.startsWith("[") && splitPk.endsWith("]")) {
                    result = splitPk.substring(1, splitPk.length() - 1).toLowerCase();
                }
                break;
            case DB2:
            case PostgreSQL:
            case KingbaseES:
            case Oscar:
                break;
            default:
                throw DataXException.asDataXException(DBUtilErrorCode.UNSUPPORTED_TYPE, "unsupported database type.");
        }

        return result;
    }


    public String quoteColumnName(String columnName) {
        String result = columnName;

        switch (this) {
            case MySql:
                result = "`" + columnName.replace("`", "``") + "`";
                break;
            case Oracle:
                break;
            case SQLServer:
                result = "[" + columnName + "]";
                break;
            case DB2:
            case PostgreSQL:
            case KingbaseES:
            case Oscar:
                break;
            default:
                throw DataXException.asDataXException(DBUtilErrorCode.UNSUPPORTED_TYPE, "unsupported database type");
        }

        return result;
    }

    public String quoteTableName(String tableName) {
        String result = tableName;

        switch (this) {
            case MySql:
                result = "`" + tableName.replace("`", "``") + "`";
                break;
            case Oracle:
                break;
            case SQLServer:
                break;
            case DB2:
                break;
            case PostgreSQL:
                break;
            case KingbaseES:
                break;
            case Oscar:
                break;
            default:
                throw DataXException.asDataXException(DBUtilErrorCode.UNSUPPORTED_TYPE, "unsupported database type");
        }

        return result;
    }

    private static Pattern mysqlPattern = Pattern.compile("jdbc:mysql://(.+):\\d+/.+");
    private static Pattern oraclePattern = Pattern.compile("jdbc:oracle:thin:@(.+):\\d+:.+");

    /**
     * 注意：目前只实现了从 mysql/oracle 中识别出ip 信息.未识别到则返回 null.
     */
    public static String parseIpFromJdbcUrl(String jdbcUrl) {
        Matcher mysql = mysqlPattern.matcher(jdbcUrl);
        if (mysql.matches()) {
            return mysql.group(1);
        }
        Matcher oracle = oraclePattern.matcher(jdbcUrl);
        if (oracle.matches()) {
            return oracle.group(1);
        }
        return null;
    }
    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

}
