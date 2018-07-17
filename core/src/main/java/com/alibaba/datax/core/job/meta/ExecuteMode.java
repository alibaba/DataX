package com.alibaba.datax.core.job.meta;

/**
 * Created by liupeng on 15/12/21.
 */
public enum ExecuteMode {
    STANDALONE("standalone"), ;

    String value;

    private ExecuteMode(String value) {
        this.value = value;
    }

    public String value() {
        return this.value;
    }

    public String getValue() {
        return this.value;
    }
}
