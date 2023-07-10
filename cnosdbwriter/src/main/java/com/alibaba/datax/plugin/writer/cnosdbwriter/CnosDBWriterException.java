package com.alibaba.datax.plugin.writer.cnosdbwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public class CnosDBWriterException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final ErrorCode errorCode;

    /**
     * @param errorCode    错误编码
     * @param errorMessage 一般使用中文描述，方便打印
     */
    public CnosDBWriterException(CnosDBWriterErrorCode errorCode, String errorMessage) {

        super(errorCode.toString() + " - " + errorMessage);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }
}
