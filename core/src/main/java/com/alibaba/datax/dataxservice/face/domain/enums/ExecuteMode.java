package com.alibaba.datax.dataxservice.face.domain.enums;

public enum ExecuteMode implements EnumStrVal {

    STANDALONE("standalone"),
    LOCAL("local"),
    DISTRIBUTE("distribute");

    String value;

    ExecuteMode(String value) {
        this.value = value;
    }

    @Override
    public String value() {
        return value;
    }

    public String getValue() {
        return this.value;
    }

    public static boolean isLocal(String mode) {
        return equalsIgnoreCase(LOCAL.getValue(), mode);
    }

    public static boolean isDistribute(String mode) {
        return equalsIgnoreCase(DISTRIBUTE.getValue(), mode);
    }

    public static ExecuteMode toExecuteMode(String modeName) {
        for (ExecuteMode mode : ExecuteMode.values()) {
            if (mode.value().equals(modeName)) {
                return mode;
            }
        }
        throw new RuntimeException("no such mode :" + modeName);
    }

    private static boolean equalsIgnoreCase(String str1, String str2) {
        return str1 == null ? str2 == null : str1.equalsIgnoreCase(str2);
    }

    @Override
    public String toString() {
        return this.value;
    }
}
