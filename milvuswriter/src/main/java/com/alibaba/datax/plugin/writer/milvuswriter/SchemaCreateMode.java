package com.alibaba.datax.plugin.writer.milvuswriter;

import lombok.Getter;

@Getter
public enum SchemaCreateMode {
    CREATE_WHEN_NOT_EXIST(0),
    EXCEPTION(1),
    IGNORE(2);

    private int mode;

    SchemaCreateMode(int mode) {
        this.mode = mode;
    }
}
