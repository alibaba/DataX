package com.alibaba.datax.plugin.reader.kudureader;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @author daizihao
 * @create 2021-01-19 15:18
 **/
public enum KuduReaderErrorcode implements ErrorCode {
    REQUIRED_VALUE("Kudureader-00", "You are missing a required parameter value."),
    ILLEGAL_VALUE("Kudureader-01", "You fill in the parameter values are not legitimate."),
    GET_KUDU_CONNECTION_ERROR("Kudureader-02", "Error getting Kudu connection."),
    GET_KUDU_TABLE_ERROR("Kudureader-03", "Error getting Kudu table.");

    private final String code;
    private final String description;

    KuduReaderErrorcode(String code, String description) {
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

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }
}
