package com.alibaba.datax.common.exception;

import java.io.PrintWriter;
import java.io.StringWriter;

public final class ExceptionTracker {
    public static final int STRING_BUFFER = 1024;

    public static String trace(Throwable ex) {
        StringWriter sw = new StringWriter(STRING_BUFFER);
        PrintWriter pw = new PrintWriter(sw);
        ex.printStackTrace(pw);
        return sw.toString();
    }
}
