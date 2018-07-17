package com.alibaba.datax.plugin.rdbms.writer;

import com.alibaba.datax.common.spi.ErrorCode;

//TODO 后续考虑与 util 包种的 DBUTilErrorCode 做合并.（区分读和写的错误码）
public enum MysqlWriterErrorCode implements ErrorCode {
    ;

    private final String code;
    private final String describe;

    private MysqlWriterErrorCode(String code, String describe) {
        this.code = code;
        this.describe = describe;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.describe;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Describe:[%s]. ", this.code,
                this.describe);
    }
}
