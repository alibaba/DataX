package com.alibaba.datax.plugin.writer.doriswriter;

import com.google.common.base.Strings;

import java.io.StringWriter;

public class DelimiterParser {

    private static final String HEX_STRING = "0123456789ABCDEF";

    public static String parse(String sp, String dSp) throws RuntimeException {
        if ( Strings.isNullOrEmpty(sp)) {
            return dSp;
        }
        if (!sp.toUpperCase().startsWith("\\X")) {
            return sp;
        }
        String hexStr = sp.substring(2);
        // check hex str
        if (hexStr.isEmpty()) {
            throw new RuntimeException("Failed to parse delimiter: `Hex str is empty`");
        }
        if (hexStr.length() % 2 != 0) {
            throw new RuntimeException("Failed to parse delimiter: `Hex str length error`");
        }
        for (char hexChar : hexStr.toUpperCase().toCharArray()) {
            if (HEX_STRING.indexOf(hexChar) == -1) {
                throw new RuntimeException("Failed to parse delimiter: `Hex str format error`");
            }
        }
        // transform to separator
        StringWriter writer = new StringWriter();
        for (byte b : hexStrToBytes(hexStr)) {
            writer.append((char) b);
        }
        return writer.toString();
    }

    private static byte[] hexStrToBytes(String hexStr) {
        String upperHexStr = hexStr.toUpperCase();
        int length = upperHexStr.length() / 2;
        char[] hexChars = upperHexStr.toCharArray();
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            int pos = i * 2;
            bytes[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
        }
        return bytes;
    }

    private static byte charToByte(char c) {
        return (byte) HEX_STRING.indexOf(c);
    }
}
