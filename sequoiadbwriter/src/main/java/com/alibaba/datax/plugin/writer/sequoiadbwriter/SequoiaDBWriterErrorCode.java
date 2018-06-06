package com.alibaba.datax.plugin.writer.sequoiadbwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum  SequoiaDBWriterErrorCode implements ErrorCode {

    ILLEGAL_VALUE("ILLEGAL_PARAMETER_VALUE","参数不合法"),
    NETWORK_ERROR("NETWORK_ERROR", "网络错误"),
    SDB_NET_CANNOT_CONNECT("SDB_NET_CANNOT_CONNECT", "网络连接失败"),
    SDB_AUTH_AUTHORITY_FORBIDDEN("SDB_AUTH_AUTHORITY_FORBIDDEN", "用户或密码错误"),
    UNEXCEPT_EXCEPTION("UNEXCEPT_EXCEPTION", "未知异常");


    private final String code;

    private final String description;

    SequoiaDBWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
