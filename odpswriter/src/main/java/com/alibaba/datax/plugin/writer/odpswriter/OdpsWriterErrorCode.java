package com.alibaba.datax.plugin.writer.odpswriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum OdpsWriterErrorCode implements ErrorCode {
    REQUIRED_VALUE("OdpsWriter-00", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("OdpsWriter-01", "您配置的值不合法."),
    UNSUPPORTED_COLUMN_TYPE("OdpsWriter-02", "DataX 不支持写入 ODPS 的目的表的此种数据类型."),

    TABLE_TRUNCATE_ERROR("OdpsWriter-03", "清空 ODPS 目的表时出错."),
    CREATE_MASTER_UPLOAD_FAIL("OdpsWriter-04", "创建 ODPS 的 uploadSession 失败."),
    GET_SLAVE_UPLOAD_FAIL("OdpsWriter-05", "获取 ODPS 的 uploadSession 失败."),
    GET_ID_KEY_FAIL("OdpsWriter-06", "获取 accessId/accessKey 失败."),
    GET_PARTITION_FAIL("OdpsWriter-07", "获取 ODPS 目的表的所有分区失败."),

    ADD_PARTITION_FAILED("OdpsWriter-08", "添加分区到 ODPS 目的表失败."),
    WRITER_RECORD_FAIL("OdpsWriter-09", "写入数据到 ODPS 目的表失败."),

    COMMIT_BLOCK_FAIL("OdpsWriter-10", "提交 block 到 ODPS 目的表失败."),
    RUN_SQL_FAILED("OdpsWriter-11", "执行 ODPS Sql 失败."),
    CHECK_IF_PARTITIONED_TABLE_FAILED("OdpsWriter-12", "检查 ODPS 目的表:%s 是否为分区表失败."),

    RUN_SQL_ODPS_EXCEPTION("OdpsWriter-13", "执行 ODPS Sql 时抛出异常, 可重试"),

    ACCOUNT_TYPE_ERROR("OdpsWriter-30", "账号类型错误."),

    PARTITION_ERROR("OdpsWriter-31", "分区配置错误."),

    COLUMN_NOT_EXIST("OdpsWriter-32", "用户配置的列不存在."),

    ODPS_PROJECT_NOT_FOUNT("OdpsWriter-100", "您配置的值不合法, odps project 不存在."),  //ODPS-0420111: Project not found

    ODPS_TABLE_NOT_FOUNT("OdpsWriter-101", "您配置的值不合法, odps table 不存在"), // ODPS-0130131:Table not found

    ODPS_ACCESS_KEY_ID_NOT_FOUND("OdpsWriter-102", "您配置的值不合法, odps accessId,accessKey 不存在"), //ODPS-0410051:Invalid credentials - accessKeyId not found

    ODPS_ACCESS_KEY_INVALID("OdpsWriter-103", "您配置的值不合法, odps accessKey 错误"), //ODPS-0410042:Invalid signature value - User signature dose not match;

    ODPS_ACCESS_DENY("OdpsWriter-104", "拒绝访问, 您不在 您配置的 project 中") //ODPS-0420095: Access Denied - Authorization Failed [4002], You doesn't exist in project

    ;

    private final String code;
    private final String description;

    private OdpsWriterErrorCode(String code, String description) {
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
