package com.alibaba.datax.plugin.reader.kudureader;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by roy on 2019/12/12 1543.
 */
public enum KuduReaderErrorCode implements ErrorCode {

    ILLEGAL_VALUE("ILLEGAL_PARAMETER_VALUE","参数不合法"),
    ILLEGAL_ADDRESS("ILLEGAL_ADDRESS","不合法的Kudu Master Addresses"),
    UNEXCEPT_EXCEPTION("UNEXCEPT_EXCEPTION","未知异常");

    private final String code;

    private final String description;

    private KuduReaderErrorCode(String code,String description) {
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

