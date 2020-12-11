package com.alibaba.datax.plugin.rdbms.util;

import com.alibaba.datax.common.spi.ErrorCode;

//TODO
public enum DBUtilErrorCode implements ErrorCode {
    //连接错误
    MYSQL_CONN_USERPWD_ERROR("MYSQLErrCode-01","数据库用户名或者密码错误，请检查填写的账号密码或者联系DBA确认账号和密码是否正确"),
    MYSQL_CONN_IPPORT_ERROR("MYSQLErrCode-02","数据库服务的IP地址或者Port错误，请检查填写的IP地址和Port或者联系DBA确认IP地址和Port是否正确。如果是同步中心用户请联系DBA确认idb上录入的IP和PORT信息和数据库的当前实际信息是一致的"),
    MYSQL_CONN_DB_ERROR("MYSQLErrCode-03","数据库名称错误，请检查数据库实例名称或者联系DBA确认该实例是否存在并且在正常服务"),

    ORACLE_CONN_USERPWD_ERROR("ORACLEErrCode-01","数据库用户名或者密码错误，请检查填写的账号密码或者联系DBA确认账号和密码是否正确"),
    ORACLE_CONN_IPPORT_ERROR("ORACLEErrCode-02","数据库服务的IP地址或者Port错误，请检查填写的IP地址和Port或者联系DBA确认IP地址和Port是否正确。如果是同步中心用户请联系DBA确认idb上录入的IP和PORT信息和数据库的当前实际信息是一致的"),
    ORACLE_CONN_DB_ERROR("ORACLEErrCode-03","数据库名称错误，请检查数据库实例名称或者联系DBA确认该实例是否存在并且在正常服务"),

    //execute query错误
    MYSQL_QUERY_TABLE_NAME_ERROR("MYSQLErrCode-04","表不存在，请检查表名或者联系DBA确认该表是否存在"),
    MYSQL_QUERY_SQL_ERROR("MYSQLErrCode-05","SQL语句执行出错，请检查Where条件是否存在拼写或语法错误"),
    MYSQL_QUERY_COLUMN_ERROR("MYSQLErrCode-06","Column信息错误，请检查该列是否存在，如果是常量或者变量，请使用英文单引号’包起来"),
    MYSQL_QUERY_SELECT_PRI_ERROR("MYSQLErrCode-07","读表数据出错，因为账号没有读表的权限，请联系DBA确认该账号的权限并授权"),

    ORACLE_QUERY_TABLE_NAME_ERROR("ORACLEErrCode-04","表不存在，请检查表名或者联系DBA确认该表是否存在"),
    ORACLE_QUERY_SQL_ERROR("ORACLEErrCode-05","SQL语句执行出错，原因可能是你填写的列不存在或者where条件不符合要求，1，请检查该列是否存在，如果是常量或者变量，请使用英文单引号’包起来;  2，请检查Where条件是否存在拼写或语法错误"),
    ORACLE_QUERY_SELECT_PRI_ERROR("ORACLEErrCode-06","读表数据出错，因为账号没有读表的权限，请联系DBA确认该账号的权限并授权"),
    ORACLE_QUERY_SQL_PARSER_ERROR("ORACLEErrCode-07","SQL语法出错，请检查Where条件是否存在拼写或语法错误"),

    //PreSql,Post Sql错误
    MYSQL_PRE_SQL_ERROR("MYSQLErrCode-08","PreSQL语法错误，请检查"),
    MYSQL_POST_SQL_ERROR("MYSQLErrCode-09","PostSql语法错误，请检查"),
    MYSQL_QUERY_SQL_PARSER_ERROR("MYSQLErrCode-10","SQL语法出错，请检查Where条件是否存在拼写或语法错误"),

    ORACLE_PRE_SQL_ERROR("ORACLEErrCode-08", "PreSQL语法错误，请检查"),
    ORACLE_POST_SQL_ERROR("ORACLEErrCode-09", "PostSql语法错误，请检查"),

    //SplitPK 错误
    MYSQL_SPLIT_PK_ERROR("MYSQLErrCode-11","SplitPK错误，请检查"),
    ORACLE_SPLIT_PK_ERROR("ORACLEErrCode-10","SplitPK错误，请检查"),

    //Insert,Delete 权限错误
    MYSQL_INSERT_ERROR("MYSQLErrCode-12","数据库没有写权限，请联系DBA"),
    MYSQL_DELETE_ERROR("MYSQLErrCode-13","数据库没有Delete权限，请联系DBA"),
    ORACLE_INSERT_ERROR("ORACLEErrCode-11","数据库没有写权限，请联系DBA"),
    ORACLE_DELETE_ERROR("ORACLEErrCode-12","数据库没有Delete权限，请联系DBA"),

    JDBC_NULL("DBUtilErrorCode-20","JDBC URL为空，请检查配置"),
    JDBC_OB10_ADDRESS_ERROR("DBUtilErrorCode-OB10-01","JDBC OB10格式错误，请联系askdatax"),
    CONF_ERROR("DBUtilErrorCode-00", "您的配置错误."),
    CONN_DB_ERROR("DBUtilErrorCode-10", "连接数据库失败. 请检查您的 账号、密码、数据库名称、IP、Port或者向 DBA 寻求帮助(注意网络环境)."),
    GET_COLUMN_INFO_FAILED("DBUtilErrorCode-01", "获取表字段相关信息失败."),
    UNSUPPORTED_TYPE("DBUtilErrorCode-12", "不支持的数据库类型. 请注意查看 DataX 已经支持的数据库类型以及数据库版本."),
    COLUMN_SPLIT_ERROR("DBUtilErrorCode-13", "根据主键进行切分失败."),
    SET_SESSION_ERROR("DBUtilErrorCode-14", "设置 session 失败."),
    RS_ASYNC_ERROR("DBUtilErrorCode-15", "异步获取ResultSet next失败."),

    REQUIRED_VALUE("DBUtilErrorCode-03", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("DBUtilErrorCode-02", "您填写的参数值不合法."),
    ILLEGAL_SPLIT_PK("DBUtilErrorCode-04", "您填写的主键列不合法, DataX 仅支持切分主键为一个,并且类型为整数或者字符串类型."),
    SPLIT_FAILED_ILLEGAL_SQL("DBUtilErrorCode-15", "DataX尝试切分表时, 执行数据库 Sql 失败. 请检查您的配置 table/splitPk/where 并作出修改."),
    SQL_EXECUTE_FAIL("DBUtilErrorCode-06", "执行数据库 Sql 失败, 请检查您的配置的 column/table/where/querySql或者向 DBA 寻求帮助."),

    // only for reader
    READ_RECORD_FAIL("DBUtilErrorCode-07", "读取数据库数据失败. 请检查您的配置的 column/table/where/querySql或者向 DBA 寻求帮助."),
    TABLE_QUERYSQL_MIXED("DBUtilErrorCode-08", "您配置凌乱了. 不能同时既配置table又配置querySql"),
    TABLE_QUERYSQL_MISSING("DBUtilErrorCode-09", "您配置错误. table和querySql 应该并且只能配置一个."),

    // only for writer
    WRITE_DATA_ERROR("DBUtilErrorCode-05", "往您配置的写入表中写入数据时失败."),
    NO_INSERT_PRIVILEGE("DBUtilErrorCode-11", "数据库没有写权限，请联系DBA"),
    NO_DELETE_PRIVILEGE("DBUtilErrorCode-16", "数据库没有DELETE权限，请联系DBA"),
    ;

    private final String code;

    private final String description;

    private DBUtilErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
