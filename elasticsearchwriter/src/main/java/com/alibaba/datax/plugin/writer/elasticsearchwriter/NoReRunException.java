package com.alibaba.datax.plugin.writer.elasticsearchwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.ErrorCode;

public class NoReRunException extends DataXException {
    public NoReRunException(String errorMessage) {
        super(errorMessage);
    }

    public NoReRunException(ErrorCode errorCode, String errorMessage) {
        super(errorCode, errorMessage);
    }

    private static final long serialVersionUID = 1L;
}