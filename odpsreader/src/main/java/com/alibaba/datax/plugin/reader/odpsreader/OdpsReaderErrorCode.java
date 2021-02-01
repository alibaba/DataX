package com.alibaba.datax.plugin.reader.odpsreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum OdpsReaderErrorCode implements ErrorCode {
    REQUIRED_VALUE("OdpsReader-00", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("OdpsReader-01", "您配置的值不合法."),
    CREATE_DOWNLOADSESSION_FAIL("OdpsReader-03", "创建 ODPS 的 downloadSession 失败."),
    GET_DOWNLOADSESSION_FAIL("OdpsReader-04", "获取 ODPS 的 downloadSession 失败."),
    READ_DATA_FAIL("OdpsReader-05", "读取 ODPS 源头表失败."),
    GET_ID_KEY_FAIL("OdpsReader-06", "获取 accessId/accessKey 失败."),

    ODPS_READ_EXCEPTION("OdpsReader-07", "读取 odps 异常"),
    OPEN_RECORD_READER_FAILED("OdpsReader-08", "打开 recordReader 失败."),

    ODPS_PROJECT_NOT_FOUNT("OdpsReader-10", "您配置的值不合法, odps project 不存在."),  //ODPS-0420111: Project not found

    ODPS_TABLE_NOT_FOUNT("OdpsReader-12", "您配置的值不合法, odps table 不存在."), // ODPS-0130131:Table not found

    ODPS_ACCESS_KEY_ID_NOT_FOUND("OdpsReader-13", "您配置的值不合法, odps accessId,accessKey 不存在."), //ODPS-0410051:Invalid credentials - accessKeyId not found

    ODPS_ACCESS_KEY_INVALID("OdpsReader-14", "您配置的值不合法, odps accessKey 错误."), //ODPS-0410042:Invalid signature value - User signature dose not match

    ODPS_ACCESS_DENY("OdpsReader-15", "拒绝访问, 您不在 您配置的 project 中."), //ODPS-0420095: Access Denied - Authorization Failed [4002], You doesn't exist in project



    SPLIT_MODE_ERROR("OdpsReader-30", "splitMode配置错误."),

    ACCOUNT_TYPE_ERROR("OdpsReader-31", "odps 账号类型错误."),

    VIRTUAL_VIEW_NOT_SUPPORT("OdpsReader-32", "Datax 不支持 读取虚拟视图."),

    PARTITION_ERROR("OdpsReader-33", "分区配置错误."),

    ;
    private final String code;
    private final String description;

    private OdpsReaderErrorCode(String code, String description) {
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
