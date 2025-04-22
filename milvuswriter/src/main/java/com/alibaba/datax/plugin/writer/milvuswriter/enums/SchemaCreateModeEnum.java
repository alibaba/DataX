package com.alibaba.datax.plugin.writer.milvuswriter.enums;

import lombok.extern.slf4j.Slf4j;

/**
 * @author ziming(子茗)
 * @date 12/27/24
 * @description
 */
@Slf4j
public enum SchemaCreateModeEnum {
    CREATEIFNOTEXIT("createIfNotExist"),
    IGNORE("ignore"),
    RECREATE("recreate");
    String type;

    SchemaCreateModeEnum(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static SchemaCreateModeEnum getEnum(String name) {
        for (SchemaCreateModeEnum value : SchemaCreateModeEnum.values()) {
            if (value.getType().equalsIgnoreCase(name)) {
                return value;
            }
        }
        log.info("use default CREATEIFNOTEXIT schame create mode");
        return CREATEIFNOTEXIT;
    }
}