package com.alibaba.datax.common.exception;

import com.alibaba.datax.common.spi.ErrorCode;

import java.io.PrintWriter;
import java.io.StringWriter;

public class DataXException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private ErrorCode errorCode;

    public DataXException(ErrorCode errorCode, String errorMessage) {
        super(errorCode.toString() + " - " + errorMessage);
        this.errorCode = errorCode;
    }

    public DataXException(String errorMessage) {
        super(errorMessage);
    }

    private DataXException(ErrorCode errorCode, String errorMessage, Throwable cause) {
        super(errorCode.toString() + " - " + getMessage(errorMessage) + " - " + getMessage(cause), cause);

        this.errorCode = errorCode;
    }

    public static DataXException asDataXException(ErrorCode errorCode, String message) {
        return new DataXException(errorCode, message);
    }

    public static DataXException asDataXException(String message) {
        return new DataXException(message);
    }

    public static DataXException asDataXException(ErrorCode errorCode, String message, Throwable cause) {
        if (cause instanceof DataXException) {
            return (DataXException) cause;
        }
        return new DataXException(errorCode, message, cause);
    }

    public static DataXException asDataXException(ErrorCode errorCode, Throwable cause) {
        if (cause instanceof DataXException) {
            return (DataXException) cause;
        }
        return new DataXException(errorCode, getMessage(cause), cause);
    }

    public ErrorCode getErrorCode() {
        return this.errorCode;
    }

    private static String getMessage(Object obj) {
        if (obj == null) {
            return "";
        }

        if (obj instanceof Throwable) {
            StringWriter str = new StringWriter();
            PrintWriter pw = new PrintWriter(str);
            ((Throwable) obj).printStackTrace(pw);
            return str.toString();
            // return ((Throwable) obj).getMessage();
        } else {
            return obj.toString();
        }
    }
}
