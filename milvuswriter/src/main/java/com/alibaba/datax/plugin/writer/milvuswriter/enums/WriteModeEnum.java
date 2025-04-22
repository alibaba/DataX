package com.alibaba.datax.plugin.writer.milvuswriter.enums;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public enum WriteModeEnum {
    INSERT("insert"),
    UPSERT("upsert");
    String mode;

    public String getMode() {
        return mode;
    }

    WriteModeEnum(String mode) {
        this.mode = mode;
    }

    public static WriteModeEnum getEnum(String mode) {
        for (WriteModeEnum writeModeEnum : WriteModeEnum.values()) {
            if (writeModeEnum.getMode().equalsIgnoreCase(mode)) {
                return writeModeEnum;
            }
        }
        log.info("use default write mode upsert");
        return UPSERT;
    }
}
